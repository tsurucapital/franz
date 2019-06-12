{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedStrings, ViewPatterns #-}
{-# LANGUAGE UndecidableInstances #-}
module Database.Franz.Network
  (startServer
  , Connection
  , withConnection
  , connect
  , disconnect
  , RequestLine(..)
  , Request(..)
  , RequestType(..)
  , defRequest
  , Response(..)
  , awaitResponse
  , ResponseLine
  , fetch
  , fetchSimple
  , FranzException(..)) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Trans.State.Strict (StateT(..), evalStateT)
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Database.Franz.Reader
import Data.Foldable (toList)
import Data.Functor.Identity
import Data.Function (fix)
import qualified Data.IntMap.Strict as IM
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

data ResponseHeader = ResponseInstant !Int
    | ResponseWait !Int -- response ID
    | ResponseDelayed !Int !Int
    | ResponseError !FranzException
    | ResponseDone
    deriving (Show, Generic)
instance Serialize ResponseHeader

respond :: FranzReader -> B.ByteString -> S.Socket -> IO ()
respond env (B.unpack -> path) conn = do
  reqs <- runGetRecv conn get :: IO [Request []]
  vResponseCounter <- newTVarIO 0
  vMain <- newTVarIO True
  tids <- forM reqs $ \(Request timeout req) -> do
    query <- sequence <$> traverse (handleRequestLine env path) req
    let ready = all (\(r,_, _) -> r)
    join $ atomically $ do
      offsets <- query
      -- main phase: return ResponseInstant or ResponseWait
      if ready offsets
        then return $ do
          SB.sendAll conn $ encode $ ResponseInstant $ length offsets
          mapM_ send offsets
          return []
        else do
          i <- readTVar vResponseCounter
          modifyTVar' vResponseCounter (+1)
          return $ do
            delay <- newDelay timeout
            SB.sendAll conn $ encode $ ResponseWait i
            fmap pure $ forkIO $ join $ atomically $ do
              -- Stand still if the main phase hasn't finished yet
              readTVar vMain >>= flip when retry
              xss <- query
              timedout <- tryWaitDelay delay
              unless (timedout || ready offsets) retry
              return $ do
                SB.sendAll conn $ encode $ ResponseDelayed i (length xss)
                mapM_ send xss
  atomically $ writeTVar vMain False
  msg <- SB.recv conn 4096
  case msg of
    "DONE" -> do
      mapM_ killThread $ concat tids
      SB.sendAll conn $ encode ResponseDone
    _ -> throwIO MalformedRequest
  where
    send (_, h, offsets) = do
      SB.sendAll conn $ encode $ length offsets
      forM_ offsets $ \(i, xs, pos, len) -> do
        SB.sendAll conn $ encode (i, HM.toList xs, len)
        SF.sendFile' conn h (fromIntegral pos) (fromIntegral len)

startServer :: S.PortNumber
    -> FilePath -- live prefix
    -> Maybe FilePath -- archive prefix
    -> IO ()
startServer port lprefix aprefix = withFranzReader lprefix $ \env -> do
  vMountCount <- newTVarIO (HM.empty :: HM.HashMap B.ByteString Int)
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock 2
    forever $ do
      (conn, _) <- S.accept sock
      forkFinally (do
        decode <$> SB.recv conn 4096 >>= \case
          Left _ -> throwIO MalformedRequest
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
            (SB.sendAll conn "READY" >> forever (respond env path conn))
              `finally` do
                join $ atomically $ do
                  m <- readTVar vMountCount
                  case HM.lookup path m of
                    Just 1 -> do
                      writeTVar vMountCount $ HM.delete path m
                      return $ callProcess "fusermount" ["-u", dest]
                    Just n -> do
                      writeTVar vMountCount $! HM.insert path (n - 1) m
                      pure (pure ())
                    Nothing -> pure (pure ())
          Right path -> do
            SB.sendAll conn "READY"
            forever $ respond env path conn
        )
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encode $ ResponseError e
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

-- The protocol for connection
--
-- Client: Let P be an archive prefix. Send P
-- Server: Receive P. If P exists, mount it. Send "READY" and start a loop.
--         If it doesn't, look for a live stream prefixed by P.
-- Client: Receive "READY"

newtype Connection = Connection (MVar S.Socket)

withConnection :: String -> S.PortNumber -> B.ByteString -> (Connection -> IO r) -> IO r
withConnection host port dir = bracket (connect host port dir) disconnect

connect :: String -> S.PortNumber -> B.ByteString -> IO Connection
connect host port dir = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ encode dir
  resp <- SB.recv sock 4096
  case resp of
    "READY" -> Connection <$> newMVar sock
    msg -> case decode msg of
      Right (ResponseError e) -> throw e
      e -> fail $ "connect: Unexpected response: " ++ show e

disconnect :: Connection -> IO ()
disconnect (Connection sock) = takeMVar sock >>= S.close

runGetRecv :: S.Socket -> Get a -> IO a
runGetRecv sock m = do
  let go (Done a _) = return a
      go (Partial cont) = do
        bs <- SB.recv sock 4096
        if B.null bs then go $ cont "" else go $ cont bs
      go (Fail _ _) = throwIO MalformedRequest
  bs <- SB.recv sock 4096
  go $ runGetPartial m bs

data Request t = Request
  { reqTimeout :: Int
  , reqSet :: t RequestLine
  } deriving Generic
instance Serialize (t RequestLine) => Serialize (Request t)

defRequest :: B.ByteString -> RequestLine
defRequest name = RequestLine
  { reqStream = name
  , reqFromIndex = Nothing
  , reqToIndex = Nothing
  , reqFrom = 0
  , reqTo = 0
  , reqType = AllItems
  }

type ResponseLine = [(Int, IndexMap Int, B.ByteString)]

data Response t = Response (t ResponseLine)
    | ResponseAwait (IO (t ResponseLine))

awaitResponse :: Response t -> IO (t ResponseLine)
awaitResponse (Response t) = pure t
awaitResponse (ResponseAwait t) = t

reconstitute :: Traversable t => t a -> ([a], [b] -> Maybe (t b))
reconstitute t = (toList t, evalStateT $ traverse rebuild t) where
  rebuild _ = StateT $ \case
    [] -> Nothing
    x : xs -> Just (x, xs)

getResponse :: Get ResponseLine
getResponse = do
  n <- get
  replicateM n $ (,,) <$> get <*> fmap HM.fromList get <*> get

fetch :: (Traversable f, Traversable t) => Connection -> f (Request t) -> (f (Response t) -> IO r) -> IO r
fetch (Connection vsock) freqs cont = modifyMVar vsock $ \sock -> do
  vPending <- newTVarIO (IM.empty :: IM.IntMap (Maybe [ResponseLine]))
  let (reqs, reshape) = reconstitute freqs
  let (reqs', reshapers) = unzip
        $ map (\(Request t xs) -> let (ys, f) = reconstitute xs in (Request t ys, f)) reqs
  SB.sendAll sock $ encode reqs'
  ms <- runGetRecv sock $ forM reshapers $ \rebuild -> get >>= \case
    ResponseInstant n -> maybe
        (fail "fetch/ResponseInstant: the shape of t doesn't match")
        (pure . Response) . rebuild <$> replicateM n getResponse
    ResponseWait i -> return $ do
      atomically $ modifyTVar vPending $ IM.insert i Nothing
      return $ ResponseAwait $ atomically $ do
        m <- readTVar vPending
        a <- maybe retry pure $ join $ IM.lookup i m
        modifyTVar' vPending (IM.delete i)
        maybe (fail "fetch/ResponseWait: the shape of t doesn't match") pure $ rebuild a
    ResponseDelayed _ _ -> fail "fetch/ResponseDelayed: unexpected response"
    ResponseDone -> fail "fetch/ResponseDone: unexpected response"
    ResponseError e -> return $ throwIO e
  rs <- sequence ms
  vDone <- newEmptyMVar
  _ <- flip forkFinally (const $ putMVar vDone ())
    $ fix $ \self -> join $ runGetRecv sock $ get >>= \case
      ResponseDelayed i len -> do
        resps <- replicateM len getResponse
        return $ do
          atomically $ modifyTVar' vPending $ IM.insert i $ Just resps
          self
      ResponseError e -> return $ throwIO e
      ResponseDone -> pure $ pure ()
      _ -> error "fetch: unexpected response"
  case reshape rs of
    Nothing -> error "fetch: the shape of f doesn't match"
    Just f -> do
      r <- cont f
      return (sock, r)
    `finally` do
      SB.sendAll sock "DONE"
      atomically $ modifyTVar' vPending (Just []<$)
      void $ takeMVar vDone


fetchSimple :: Connection -> Int -> RequestLine -> IO ResponseLine
fetchSimple conn timeout req = runIdentity <$> fetch conn
  (Identity $ Request timeout $ Identity req) (awaitResponse . runIdentity)
