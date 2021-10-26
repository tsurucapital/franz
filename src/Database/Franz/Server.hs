{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
module Database.Franz.Server
  ( Settings(..)
  , startServer
  , defaultPort
  ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Cont
import Control.Retry
import Control.Concurrent.STM
import Database.Franz.Internal
import Database.Franz.Protocol
import Database.Franz.Reader
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
import System.FilePath
import System.IO
import System.Process (ProcessHandle, spawnProcess, cleanupProcess,
  waitForProcess, getProcessExitCode, getPid)

data Env = Env
  { prefix :: FilePath
  , path :: FilePath
  , franzReader :: FranzReader
  , refThreads :: IORef (IM.IntMap ThreadId) -- ^ thread pool of pending requests
  , recvBuffer :: IORef B.ByteString -- ^ received but unconsumed bytes
  , vConn :: MVar S.Socket -- ^ connection to the client
  }

handleRaw :: Env -> RawRequest -> IO ()
handleRaw env@Env{..} (RawRequest reqId req) = do
  let pop result = do
        case result of
          Left ex | Just e <- fromException ex -> sendHeader env $ ResponseError reqId e
          _ -> pure ()
        `finally` popThread env reqId
  handleQuery prefix franzReader path req $ \stream query ->
    atomically (fmap Right query `catchSTM` (pure . Left)) >>= \case
      Left e -> sendHeader env $ ResponseError reqId e
      Right (ready, offsets)
        | ready -> sendContents env (Response reqId) stream offsets
        | otherwise -> do
          tid <- flip forkFinally pop $ bracket_
            (atomically $ addActivity stream)
            (removeActivity stream) $ do
              sendHeader env $ ResponseWait reqId
              offsets' <- atomically $ do
                (ready', offsets') <- query
                check ready'
                pure offsets'
              sendContents env (Response reqId) stream offsets'
          -- Store the thread ID of the thread yielding a future
          -- response such that we can kill it mid-way if user
          -- sends a cancel request or we're killed with an
          -- exception.
          atomicModifyIORef' refThreads $ \m -> (IM.insert reqId tid m, ())
        -- Response is not ready but the user indicated that they
        -- are not interested in waiting either. While we have no
        -- work left to do, we do want to send a message back
        -- saying the response would be a delayed one so that the
        -- user can give up waiting.
        | otherwise -> sendHeader env $ ResponseWait reqId
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
  , livePrefix :: FilePath
  , archivePrefix :: Maybe FilePath
  , mountPrefix :: FilePath
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
    forM_ archivePrefix $ \path -> do
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

  let respondLoop prefix path = do
        SB.sendAll conn apiVersion
        logServer [show connAddr, show path]
        refThreads <- newIORef IM.empty
        vConn <- newMVar conn
        forever (respond Env{..}) `finally` do
          readIORef refThreads >>= mapM_ killThread

  path <- liftIO $ runGetRecv recvBuffer conn get >>= \case
    Left _ -> throwIO $ MalformedRequest "Expecting a path"
    Right pathBS -> pure $ B.unpack pathBS

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
    Just prefix | src <- prefix </> path -> do
      -- check if an archive exists
      exist <- doesFileExist src
      if exist
        then withFuse vMounts closeGroup src (mountPrefix </> path) $ respondLoop mountPrefix path
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
  (mountFuse src dst)
  (\fuse -> do
    release
    killFuse fuse dst `finally` do
      pid <- getPid fuse
      forM_ pid $ \p -> logServer ["Undead squashfuse detected:", show p])
  (const body)

mountFuse :: FilePath -> FilePath -> IO ProcessHandle
mountFuse src dest = do
  createDirectoryIfMissing True dest
  logServer ["squashfuse", "-f", src, dest]
  bracketOnError (spawnProcess "squashfuse" ["-f", src, dest]) (flip killFuse dest) $ \fuse -> do
    -- It keeps process handles so that mounted directories are cleaned up
    -- but there's no easy way to tell when squashfuse finished mounting.
    -- Wait until the destination becomes non-empty.
    notMounted <- retrying (limitRetries 5 <> exponentialBackoff 100000) (const pure)
      $ \status -> getProcessExitCode fuse >>= \case
        Nothing -> do
          logServer ["Waiting for squashfuse to mount", src, ":", show status]
          null <$> listDirectory dest
        Just e -> do
          removeDirectory dest
          throwIO $ InternalError $ "squashfuse exited with " <> show e
    when notMounted $ throwIO $ InternalError $ "Failed to mount " <> src
    return fuse

killFuse :: ProcessHandle -> FilePath -> IO ()
killFuse fuse path = do
  cleanupProcess (Nothing, Nothing, Nothing, fuse)
  e <- waitForProcess fuse
  logServer ["squashfuse:", show e]
  removeDirectory path
