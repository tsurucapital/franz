{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedStrings, ViewPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveFunctor #-}
module Database.Franz.Network
  ( startServer
  , defaultPort
  , Connection
  , withConnection
  , connect
  , disconnect
  , Query(..)
  , ItemRef(..)
  , RequestType(..)
  , defQuery
  , Response
  , awaitResponse
  , SomeIndexMap
  , Contents
  , fetch
  , fetchTraverse
  , fetchSimple
  , atomicallyWithin
  , FranzException(..)) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Trans.Cont (ContT(..))
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Database.Franz.Reader
import qualified Data.IntMap.Strict as IM
import Data.IORef
import Data.Int (Int64)
import Data.Serialize
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import GHC.Generics (Generic)
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.Directory
import System.FilePath
import System.IO
import System.Process (ProcessHandle, readProcess, spawnProcess, terminateProcess, waitForProcess)

defaultPort :: S.PortNumber
defaultPort = 1886

data RawRequest = RawRequest !ResponseId !Query
    | RawClean !ResponseId deriving Generic
instance Serialize RawRequest

type ResponseId = Int

data ResponseHeader = ResponseInstant !ResponseId
    -- ^ response ID, number of streams; there are items satisfying the query
    | ResponseWait !ResponseId -- ^ response ID; requested elements are not available right now
    | ResponseDelayed !ResponseId -- ^ response ID, number of streams; items are available
    | ResponseError !ResponseId !FranzException -- ^ something went wrong
    deriving (Show, Generic)
instance Serialize ResponseHeader

-- | Initial seqno, final seqno, base offset, index names
data PayloadHeader = PayloadHeader !Int !Int !Int ![B.ByteString]

instance Serialize PayloadHeader where
  put (PayloadHeader s t u xs) = f s *> f t *> f u *> put xs where
    f = putInt64le . fromIntegral
  get = PayloadHeader <$> f <*> f <*> f <*> get where
    f = fromIntegral <$> getInt64le

respond :: FranzReader
  -> IORef (IM.IntMap ThreadId)
  -> FilePath
  -> IORef B.ByteString
  -> MVar S.Socket -> IO ()
respond env refThreads path buf vConn = do
  recvConn <- readMVar vConn
  runGetRecv buf recvConn get >>= \case
    Right (RawRequest reqId req) -> do
      (stream, query) <- handleQuery env path req
      join $ atomically $ do
        (ready, offsets) <- query
        return $ if ready
          then removeActivity stream >> send (ResponseInstant reqId) stream offsets
          else do
            m <- readIORef refThreads
            if IM.member reqId m
              then sendHeader $ ResponseError reqId $ MalformedRequest "duplicate request ID"
              else do
                sendHeader $ ResponseWait reqId
                -- Fork a thread to send a delayed response
                tid <- flip forkFinally (const $ removeActivity stream)
                  $ join $ atomically $ do
                    (ready', offsets') <- query
                    check ready'
                    return $ send (ResponseDelayed reqId) stream offsets'
                writeIORef refThreads $! IM.insert reqId tid m
        `catchSTM` \e -> return $ do
          removeActivity stream
          sendHeader $ ResponseError reqId e
      `catch` \e -> sendHeader $ ResponseError reqId e
    Right (RawClean reqId) -> do
      m <- readIORef refThreads
      mapM_ killThread $ IM.lookup reqId m
      writeIORef refThreads $! IM.delete reqId m
    Left err -> throwIO $ MalformedRequest err
  where
    sendHeader x = withMVar vConn $ \conn -> SB.sendAll conn $ encode x
    send header Stream{..} ((s0, p0), (s1, p1)) = withMVar vConn $ \conn -> do
      SB.sendAll conn $ encode (header, PayloadHeader s0 s1 p0 indexNames)
      let siz = 8 * (length indexNames + 1)
      SF.sendFile' conn indexHandle (fromIntegral $ siz * succ s0) (fromIntegral $ siz * (s1 - s0))
      SF.sendFile' conn payloadHandle (fromIntegral p0) (fromIntegral $ p1 - p0)

startServer
    :: Double -- reaping interval
    -> Double -- stream life (seconds)
    -> S.PortNumber
    -> FilePath -- live prefix
    -> Maybe FilePath -- archive prefix
    -> IO ()
startServer interval life port lprefix aprefix = withFranzReader lprefix $ \env -> do

  hSetBuffering stderr LineBuffering
  _ <- forkIO $ reaper interval life env

  vMountCount <- newTVarIO (HM.empty :: HM.HashMap FilePath (ProcessHandle, Int))
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) S.Stream (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock S.maxListenQueue
    logServer ["Listening on", show port]

    forever $ do
      (conn, connAddr) <- S.accept sock
      let respondLoop path = do
            SB.sendAll conn apiVersion
            logServer [show connAddr, show path]
            ref <- newIORef IM.empty
            buf <- newIORef B.empty
            vConn <- newMVar conn
            forever (respond env ref path buf vConn) `finally` do
              readIORef ref >>= mapM_ killThread

      forkFinally (do
        decode <$> SB.recv conn 4096 >>= \case
          Left _ -> throwIO $ MalformedRequest "Expecting a path"
          Right pathBS | Just apath <- aprefix -> do
            let path = B.unpack pathBS
                src = apath </> path
                dest = lprefix </> path

            -- Mount a squashfs image and increment the counter
            join $ atomically $ do
              m <- readTVar vMountCount
              case HM.lookup path m of
                Nothing -> return $ do
                  b <- doesFileExist src
                  when b $ do
                    createDirectoryIfMissing True dest
                    logServer ["squashfuse", "-f", src, dest]
                    fuse <- spawnProcess "squashfuse" ["-f", src, dest]
                    threadDelay 100000
                    atomically $ writeTVar vMountCount $! HM.insert path (fuse, 1) m
                Just (_, 0) -> retry -- it's being closed unfortunately
                Just (fuse, n) -> fmap pure $ writeTVar vMountCount
                  $ HM.insert path (fuse, n + 1) m

            respondLoop path
              `finally` do
                m0 <- readTVarIO vMountCount
                logServer [show connAddr, "disconnected", show $ fmap snd m0]
                join $ atomically $ do
                  m <- readTVar vMountCount
                  case HM.lookup path m of
                    Just (_, 0) -> pure $ pure () -- someone else is closing
                    Just (fuse, 1) -> do
                      writeTVar vMountCount $ HM.insert path (fuse, 0) m
                      return $ do
                        -- close the last client's streams
                        streams <- atomically $ do
                          ss <- readTVar $ vStreams env
                          writeTVar (vStreams env) $ HM.delete path ss
                          return ss
                        forM_ (HM.lookup path streams) $ mapM_ closeStream
                        terminateProcess fuse
                        _ <- waitForProcess fuse
                        callProcess' "rmdir" [dest]
                        atomically $ writeTVar vMountCount $ HM.delete path m
                    Just (fuse, n) -> do
                      writeTVar vMountCount $! HM.insert path (fuse, n - 1) m
                      pure (pure ())
                    Nothing -> pure (pure ())
          Right path -> respondLoop $ B.unpack path
        )
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encode $ ResponseError (-1) e
              Nothing -> logServer [show ex]
            Right _ -> return ()
          S.close conn
  where
    callProcess' exe args = do
        logServer $ exe : args
        out <- readProcess exe args ""
        forM_ (lines out) $ logServer . pure
    logServer = hPutStrLn stderr . unwords . (:) "[server]"

-- The protocol
--
-- Client                     Server
---  | ---- Archive prefix ---> |  Mounts P if possible
---  | <--- apiVersion -------- |
---  | ---- RawRequest i p ---> |
---  | ---- RawRequest j q ---> |
---  | ---- RawRequest k r ---> |
---  | <--- ResponseInstant i - |
---  | <--- result for p -----  |
---  | <--- ResponseWait j ---- |
---  | <--- ResponseWait k ---- |
---  | <--- ResponseDelayed j - |
---  | <--- result for q -----  |
--   | ----  RawClean i ---->   |
--   | ----  RawClean j ---->   |
--   | ----  RawClean k ---->   |

data Connection = Connection
  { connSocket :: MVar S.Socket
  , connReqId :: TVar Int
  , connStates :: TVar (IM.IntMap (ResponseStatus Contents))
  , connThread :: !ThreadId
  }

data ResponseStatus a = WaitingInstant
    | WaitingDelayed
    | Errored !FranzException
    | Available !a
    deriving (Show, Functor)

withConnection :: String -> S.PortNumber -> B.ByteString -> (Connection -> IO r) -> IO r
withConnection host port dir = bracket (connect host port dir) disconnect

apiVersion :: B.ByteString
apiVersion = "0"

connect :: String -> S.PortNumber -> B.ByteString -> IO Connection
connect host port dir = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) S.Stream (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ encode dir
  readyMsg <- SB.recv sock 4096
  unless (readyMsg == apiVersion) $ case decode readyMsg of
    Right (ResponseError _ e) -> throwIO e
    e -> throwIO $ ClientError $ "Database.Franz.Network.connect: Unexpected response: " ++ show e

  connSocket <- newMVar sock
  connReqId <- newTVarIO 0
  connStates <- newTVarIO IM.empty
  buf <- newIORef B.empty
  connThread <- flip forkFinally (either throwIO pure) $ forever
    $ (>>=either (throwIO . ClientError) atomically) $ runGetRecv buf sock $ get >>= \case
      ResponseInstant i -> do
        resp <- getResponse
        return $ do
          m <- readTVar connStates
          case IM.lookup i m of
            Nothing -> pure ()
            Just WaitingInstant -> writeTVar connStates $! IM.insert i (Available resp) m
            e -> throwSTM $ ClientError $ "Unexpected state on ResponseInstant " ++ show i ++ ": " ++ show e
      ResponseWait i -> return $ do
        m <- readTVar connStates
        case IM.lookup i m of
          Nothing -> pure ()
          Just WaitingInstant -> writeTVar connStates $! IM.insert i WaitingDelayed m
          e -> throwSTM $ ClientError $ "Unexpected state on ResponseWait " ++ show i ++ ": " ++ show e
      ResponseDelayed i -> do
        resp <- getResponse
        return $ do
          m <- readTVar connStates
          case IM.lookup i m of
            Nothing -> pure ()
            Just WaitingDelayed -> writeTVar connStates $! IM.insert i (Available resp) m
            e -> throwSTM $ ClientError $ "Unexpected state on ResponseDelayed " ++ show i ++ ": " ++ show e
      ResponseError i e -> return $ do
        m <- readTVar connStates
        case IM.lookup i m of
          Nothing -> throwSTM e
          Just _ -> writeTVar connStates $! IM.insert i (Errored e) m
  return Connection{..}

disconnect :: Connection -> IO ()
disconnect Connection{..} = do
  killThread connThread
  withMVar connSocket S.close

runGetRecv :: IORef B.ByteString -> S.Socket -> Get a -> IO (Either String a)
runGetRecv refBuf sock m = do
  lo <- readIORef refBuf
  let go (Done a lo') = do
        writeIORef refBuf lo'
        return $ Right a
      go (Partial cont) = SB.recv sock 4096 >>= go . cont
      go (Fail str lo') = do
        writeIORef refBuf lo'
        return $ Left $ show sock ++ str
  bs <- if B.null lo
    then SB.recv sock 4096
    else pure lo
  go $ runGetPartial m bs

defQuery :: B.ByteString -> Query
defQuery name = Query
  { reqStream = name
  , reqFrom = BySeqNum 0
  , reqTo = BySeqNum 0
  , reqType = AllItems
  }

type SomeIndexMap = HM.HashMap B.ByteString Int64

-- | (seqno, indices, payloads)
type Contents = [(Int, SomeIndexMap, B.ByteString)]

-- | When it is 'Right', it might block until the content arrives.
type Response = Either Contents (STM Contents)

awaitResponse :: STM (Either a (STM a)) -> STM a
awaitResponse = (>>=either pure id)

getResponse :: Get Contents
getResponse = do
  PayloadHeader s0 s1 p0 names <- get
  ixs <- V.replicateM (s1 - s0) $ (,) <$> fmap fromIntegral getInt64le <*> traverse (const getInt64le) names
  let ofss = V.cons p0 $ V.map fst ixs
  payload <- getByteString $ fromIntegral $ V.last ofss - p0
  return $ do
    i <- [0..s1-s0-1]
    let ofs0 = maybe (error "ofs0") id $ ofss V.!? i
    let ofs1 = maybe (error "ofs1") fst $ ixs V.!? i
    let indices = maybe (error "indices") snd $ ixs V.!? i
    pure (s0 + i + 1, HM.fromList $ zip names indices, B.take (ofs1 - ofs0) $ B.drop (ofs0 - p0) payload)


-- | Fetch requested data from the server.
-- Termination of the continuation cancels the request, allowing flexible
-- control of its lifetime.
fetch :: Connection
  -> Query
  -> (STM Response -> IO r) -- ^ running the STM action blocks until the response arrives
  -> IO r
fetch Connection{..} req cont = do
  reqId <- atomically $ do
    i <- readTVar connReqId
    writeTVar connReqId $! i + 1
    modifyTVar' connStates $ IM.insert i WaitingInstant
    return i
  withMVar connSocket $ \sock -> SB.sendAll sock $ encode $ RawRequest reqId req
  let
    go = do
      m <- readTVar connStates
      case IM.lookup reqId m of
        Nothing -> return $ Left [] -- fetch ended; nothing to return
        Just WaitingInstant -> retry -- wait for an instant response
        Just (Available xs) -> do
          writeTVar connStates $! IM.delete reqId m
          return $ Left xs
        Just WaitingDelayed -> return $ Right $ do
          m' <- readTVar connStates
          case IM.lookup reqId m' of
            Nothing -> return [] -- fetch ended; nothing to return
            Just WaitingDelayed -> retry
            Just (Available xs) -> do
              writeTVar connStates $! IM.delete reqId m'
              return xs
            Just (Errored e) -> throwSTM e
            Just WaitingInstant -> throwSTM $ ClientError $ "fetch/WaitingDelayed: unexpected state WaitingInstant"
        Just (Errored e) -> throwSTM e
  cont go `finally` do
    withMVar connSocket $ \sock -> do
      atomically $ modifyTVar' connStates $ IM.delete reqId
      SB.sendAll sock $ encode $ RawClean reqId

-- | Queries in traversable @t@ form an atomic request. The response will become
-- available once all the elements are available.
--
-- Generalisation to Traversable guarantees that the response preserves the
-- shape of the request.
fetchTraverse :: Traversable t => Connection -> t Query -> (STM (Either (t Contents) (STM (t Contents))) -> IO r) -> IO r
fetchTraverse conn reqs = runContT $ do
  tresps <- traverse (ContT . fetch conn) reqs
  return $ do
    resps <- sequence tresps
    case traverse (either Just (const Nothing)) resps of
      Just instant -> return $ Left instant
      Nothing -> return $ Right $ traverse (either pure id) resps

-- | Send a single query and wait for the result. If it timeouts, it returns an empty list.
fetchSimple :: Connection
  -> Int -- ^ timeout in microseconds
  -> Query
  -> IO Contents
fetchSimple conn timeout req = fetch conn req (fmap (maybe [] id) . atomicallyWithin timeout . awaitResponse)

atomicallyWithin :: Int -- ^ timeout in microseconds
  -> STM a
  -> IO (Maybe a)
atomicallyWithin timeout m = do
  d <- newDelay timeout
  atomically $ fmap Just m `orElse` (Nothing <$ waitDelay d)
