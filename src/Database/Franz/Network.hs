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
import GHC.Generics (Generic)
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.Directory
import System.FilePath
import System.IO
import System.Process (callProcess)

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

respond :: FranzReader -> IORef (IM.IntMap ThreadId) -> B.ByteString -> IORef B.ByteString -> S.Socket -> IO ()
respond env refThreads (B.unpack -> path) buf conn = runGetRecv buf conn get >>= \case
  Right (RawRequest reqId req) -> do
    query <- handleQuery env path req
    join $ atomically $ do
      (ready, h, names, offsets) <- query
      return $ if ready
        then do
          sendHeader $ ResponseInstant reqId
          send h names offsets
        else do
          m <- readIORef refThreads
          if IM.member reqId m
            then sendHeader $ ResponseError reqId $ MalformedRequest "duplicate request ID"
            else do
              sendHeader $ ResponseWait reqId
              -- Fork a thread to send a delayed response
              tid <- forkIO $ join $ atomically $ do
                (ready', h', names', offsets') <- query
                check ready'
                return $ do
                  sendHeader $ ResponseDelayed reqId
                  send h' names' offsets'
              writeIORef refThreads $! IM.insert reqId tid m
  Right (RawClean reqId) -> do
    m <- readIORef refThreads
    mapM_ killThread $ IM.lookup reqId m
    writeIORef refThreads $! IM.delete reqId m
  Left err -> throwIO $ MalformedRequest err
  where
    sendHeader = SB.sendAll conn . encode
    send h names ((s0, p0), (s1, p1)) = do
      SB.sendAll conn $ encode (s0, s1, names)
      SF.sendFile' conn h (fromIntegral p0) (fromIntegral $ p1 - p0)

startServer :: S.PortNumber
    -> FilePath -- live prefix
    -> Maybe FilePath -- archive prefix
    -> IO ()
startServer port lprefix aprefix = withFranzReader lprefix $ \env -> do
  vMountCount <- newTVarIO (HM.empty :: HM.HashMap B.ByteString Int)
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) S.Stream (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock S.maxListenQueue

    forever $ do
      (conn, _) <- S.accept sock

      let respondLoop path = do
            ref <- newIORef IM.empty
            buf <- newIORef B.empty
            forever (respond env ref path buf conn) `finally` do
              readIORef ref >>= mapM_ killThread

      forkFinally (do
        decode <$> SB.recv conn 4096 >>= \case
          Left _ -> throwIO $ MalformedRequest "Expecting a path"
          Right path | Just apath <- aprefix -> do
            let src = apath </> B.unpack path
            let dest = lprefix </> B.unpack path
            join $ atomically $ do
              m <- readTVar vMountCount
              case HM.lookup path m of
                Nothing -> return $ do
                  b <- doesFileExist src
                  when b $ do
                    createDirectoryIfMissing True dest
                    callProcess "squashfuse" [src, dest]
                    atomically $ writeTVar vMountCount $! HM.insert path 1 m
                Just n -> fmap pure $ writeTVar vMountCount $ HM.insert path (n + 1) m
            (SB.sendAll conn "READY" >> respondLoop path)
              `finally` do
                join $ atomically $ do
                  m <- readTVar vMountCount
                  case HM.lookup path m of
                    Just 1 -> return $ do
                      callProcess "fusermount" ["-u", dest]
                      atomically $ writeTVar vMountCount $ HM.delete path m
                    Just n -> do
                      writeTVar vMountCount $! HM.insert path (n - 1) m
                      pure (pure ())
                    Nothing -> pure (pure ())
          Right path -> do
            SB.sendAll conn "READY"
            respondLoop path
        )
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encode $ ResponseError (-1) e
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

-- The protocol for connection
--
-- Client: Let P be an archive prefix. Send P
-- Server: Receive P. If P exists, mount it. Send "READY" and start a loop.
--         If it doesn't, look for a live stream prefixed by P.
-- Client: Receive "READY"

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

connect :: String -> S.PortNumber -> B.ByteString -> IO Connection
connect host port dir = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) S.Stream (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ encode dir
  readyMsg <- SB.recv sock 4096
  case readyMsg of
    "READY" -> do
      connSocket <- newMVar sock
      connReqId <- newTVarIO 0
      connStates <- newTVarIO IM.empty
      buf <- newIORef B.empty
      connThread <- flip forkFinally (either throwIO pure) $ forever
        $ (>>=either fail atomically) $ runGetRecv buf sock $ get >>= \case
          ResponseInstant i -> do
            resp <- getResponse
            return $ do
              m <- readTVar connStates
              case IM.lookup i m of
                Just WaitingInstant -> writeTVar connStates $! IM.insert i (Available resp) m
                e -> fail $ "Unexpected state on ResponseInstant " ++ show i ++ ": " ++ show e
          ResponseWait i -> return $ do
            m <- readTVar connStates
            case IM.lookup i m of
              Just WaitingInstant -> writeTVar connStates $! IM.insert i WaitingDelayed m
              e -> fail $ "Unexpected state on ResponseWait " ++ show i ++ ": " ++ show e
          ResponseDelayed i -> do
            resp <- getResponse
            return $ do
              m <- readTVar connStates
              case IM.lookup i m of
                Just WaitingDelayed -> writeTVar connStates $! IM.insert i (Available resp) m
                e -> fail $ "Unexpected state on ResponseDelayed " ++ show i ++ ": " ++ show e
          ResponseError i e -> return $ do
            m <- readTVar connStates
            case IM.lookup i m of
              Nothing -> throwSTM e
              Just _ -> writeTVar connStates $! IM.insert i (Errored e) m
      return Connection{..}
    _ -> case decode readyMsg of
      Right (ResponseError _ e) -> throwIO e
      e -> fail $ "Database.Franz.Network.connect: Unexpected response: " ++ show e

disconnect :: Connection -> IO ()
disconnect Connection{..} = do
  killThread connThread
  takeMVar connSocket >>= S.close

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
  , reqFromIndex = Nothing
  , reqToIndex = Nothing
  , reqFrom = 0
  , reqTo = 0
  , reqType = AllItems
  }

type SomeIndexMap = HM.HashMap B.ByteString Int64

type Contents = [(Int, SomeIndexMap, B.ByteString)]

-- | When it is 'Right', it might block until the content arrives.
type Response = Either Contents (STM Contents)

awaitResponse :: STM (Either a (STM a)) -> STM a
awaitResponse = (>>=either pure id)

getResponse :: Get Contents
getResponse = do
  (s0, s1, names) <- get
  forM [s0+1..s1] $ \i -> do
    len <- getInt64le
    ixs <- traverse (\k -> (,) k <$> getInt64le) names
    bs <- getByteString (fromIntegral len)
    return (i, HM.fromList ixs, bs)

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
            Just WaitingInstant -> fail $ "fetch/WaitingDelayed: unexpected state WaitingInstant"
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

-- | Send a single query and wait for the result.
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
