{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
module Database.Franz.Server
  ( Settings(..)
  , startServer
  , FranzPrefix(..)
  , defaultPort
  ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Cont
import Control.Concurrent.STM
import Database.Franz.Internal.Fuse
import Database.Franz.Internal.IO
import Database.Franz.Internal.Protocol
import Database.Franz.Internal.Reader
import Data.ConcurrentResourceMap
import Data.Serialize
import qualified Data.IntMap.Strict as IM
import Data.IORef
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import Data.Tuple (swap)
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.Directory
import System.IO
import System.Process (ProcessHandle, getPid)

data Env = Env
  { prefix :: FranzPrefix
  , path :: FranzDirectory
  , franzReader :: FranzReader
  , refThreads :: IORef (IM.IntMap ThreadId) -- ^ thread pool of pending requests
  , recvBuffer :: IORef B.ByteString -- ^ received but unconsumed bytes
  , vConn :: MVar S.Socket -- ^ connection to the client
  }

trySTM :: Exception e => STM a -> STM (Either e a)
trySTM m = fmap Right m `catchSTM` (pure . Left)

handleRaw :: Env -> RawRequest -> IO ()
handleRaw env@Env{..} (RawRequest reqId req) = do
  let pop result = do
        case result of
          Left ex | Just e <- fromException ex -> sendHeader env $ ResponseError reqId e
          _ -> pure ()
        `finally` popThread env reqId
  handleQuery prefix franzReader path req throwIO $ \stream query ->
    atomically (trySTM query) >>= \case
      Left e -> sendHeader env $ ResponseError reqId e
      Right (Just offsets) -> sendContents env (Response reqId) stream offsets
      Right Nothing -> do
          tid <- flip forkFinally pop $ bracket_
            (atomically $ addActivity stream)
            (removeActivity stream) $ do
              sendHeader env $ ResponseWait reqId
              join $ atomically $ trySTM query >>= \case
                Left e -> pure $ sendHeader env $ ResponseError reqId e
                Right Nothing -> retry
                Right (Just offsets) ->  
                  pure $ sendContents env (Response reqId) stream offsets
          -- Store the thread ID of the thread yielding a future
          -- response such that we can kill it mid-way if user
          -- sends a cancel request or we're killed with an
          -- exception.
          atomicModifyIORef' refThreads $ \m -> (IM.insert reqId tid m, ())
  `catch` \e -> sendHeader env $ ResponseError reqId e

handleRaw env (RawClean reqId) = do
  tid <- popThread env reqId
  mapM_ killThread tid

-- | Pick up a 'ThreadId' from a pool.
popThread :: Env -> Int -> IO (Maybe ThreadId)
popThread Env{..} reqId = atomicModifyIORef' refThreads
  $ swap . IM.updateLookupWithKey (\_ _ -> Nothing) reqId

sendHeader :: Env -> ResponseHeader -> IO ()
sendHeader Env{..} x = withMVar vConn $ \conn -> SB.sendAll conn $ encode x

sendContents :: Env -> ResponseHeader -> Stream -> QueryResult -> IO ()
sendContents Env{..} header Stream{..} ((s0, p0), (s1, p1)) = withMVar vConn $ \conn -> do
  SB.sendAll conn $ encode (header, PayloadHeader s0 s1 p0 indexNames)
  -- byte offset + number of indices
  let siz = 8 * (length indexNames + 1)
  -- Send byte offsets and indices
  SF.sendFile' conn indexHandle (fromIntegral $ siz * succ s0) (fromIntegral $ siz * (s1 - s0))
  -- Send payloads
  SF.sendFile' conn payloadHandle (fromIntegral p0) (fromIntegral $ p1 - p0)

respond :: Env -> IO ()
respond env@Env{..} = do
  recvConn <- readMVar vConn
  runGetRecv recvBuffer recvConn get >>= \case
    Right raw -> handleRaw env raw
    Left err -> throwIO $ MalformedRequest err

data Settings = Settings
  { reapInterval :: Double
  , streamLifetime :: Double
  , port :: S.PortNumber
  , livePrefix :: FranzPrefix
  , archivePrefix :: Maybe FranzPrefix
  , mountPrefix :: FranzPrefix
  }

newtype MountMap v = MountMap (HM.HashMap FilePath v)

instance ResourceMap MountMap where
  type Key MountMap = FilePath
  empty = MountMap mempty
  delete k (MountMap m) = MountMap (HM.delete k m)
  insert k v (MountMap m) = MountMap (HM.insert k v m)
  lookup k (MountMap m) = HM.lookup k m

newMountMap :: IO (ConcurrentResourceMap MountMap ProcessHandle)
newMountMap = newResourceMap

startServer
    :: Settings
    -> IO ()
startServer Settings{..} = evalContT $ do
  franzReader <- ContT withFranzReader

  liftIO $ do
    forM_ archivePrefix $ \(FranzPrefix path) -> do
      e <- doesDirectoryExist path
      unless e $ error $ "archive prefix " ++ path ++ " doesn't exist"

    _ <- liftIO $ forkIO $ reaper reapInterval streamLifetime franzReader
    hSetBuffering stderr LineBuffering

  vMounts <- liftIO newMountMap

  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- liftIO $ S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  -- obtain a socket and start listening
  sock <- ContT $ bracket (S.socket (S.addrFamily addr) S.Stream (S.addrProtocol addr)) S.close

  liftIO $ do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.addrAddress addr
    S.listen sock S.maxListenQueue

  logServer ["Listening on", show port]

  liftIO $ forever $ do
    (conn, connAddr) <- S.accept sock

    void $ forkFinally (accept Settings{..} vMounts franzReader conn connAddr) $ \result -> do
      case result of
        Left ex -> case fromException ex of
          Just e -> SB.sendAll conn $ encode $ ResponseError (-1) e
          Nothing -> logServer [show ex]
        Right _ -> return ()
      S.close conn
      logServer [show connAddr, "disconnected"]

accept :: Settings -> ConcurrentResourceMap MountMap ProcessHandle -> FranzReader -> S.Socket -> S.SockAddr -> IO ()
accept Settings{..} vMounts franzReader conn connAddr = do
  -- buffer of received octets
  recvBuffer <- newIORef B.empty

  let respondLoop prefix path@(FranzDirectory dir) = do
        SB.sendAll conn apiVersion
        logServer [show connAddr, show dir]
        refThreads <- newIORef IM.empty
        vConn <- newMVar conn
        forever (respond Env{..}) `finally` do
          readIORef refThreads >>= mapM_ killThread

  path <- liftIO $ runGetRecv recvBuffer conn get >>= \case
    Left _ -> throwIO $ MalformedRequest "Expecting a path"
    Right pathBS -> pure $ FranzDirectory $ B.unpack pathBS

  -- when the final reader exits, close all the streams associated to the path
  let closeGroup = do
        streams <- atomically $ do
          streams <- readTVar $ vStreams franzReader
          writeTVar (vStreams franzReader) $ HM.delete path streams
          pure streams
        forM_ (HM.lookup path streams) $ mapM_ closeStream

  case archivePrefix of
    -- just start a session without thinking about archives
    Nothing -> respondLoop livePrefix path
    -- Mount a squashfs image and increment the counter
    Just prefix | src <- getFranzDirectory prefix path -> do
      -- check if an archive exists
      exist <- doesFileExist src
      if exist
        then withFuse vMounts closeGroup src (getFranzDirectory mountPrefix path) $ respondLoop mountPrefix path
        else do
          logServer ["Archive", src, "doesn't exist; falling back to live streams"]
          respondLoop livePrefix path

logServer :: MonadIO m => [String] -> m ()
logServer = liftIO . hPutStrLn stderr . unwords . (:) "[server]"

withFuse :: ConcurrentResourceMap MountMap ProcessHandle
  -> IO () -- run when the final user is about to exit
  -> FilePath
  -> FilePath
  -> IO a -> IO a
withFuse vMounts release src dst body = withSharedResource vMounts dst
  (mountFuse logServer (throwIO . InternalError) src dst)
  (\fuse -> do
    release
    killFuse logServer fuse dst `finally` do
      pid <- getPid fuse
      forM_ pid $ \p -> logServer ["Undead squashfuse detected:", show p])
  (const body)
