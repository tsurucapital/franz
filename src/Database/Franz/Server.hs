{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE BangPatterns #-}
module Database.Franz.Server
  ( Settings(..)
  , startServer
  , defaultPort
  ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
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
      touchActivity stream
      atomically (fmap Right query `catchSTM` (pure . Left)) >>= \case
        Left e -> sendHeader $ ResponseError reqId e
        Right (ready, offsets)
          | ready -> send (ResponseInstant reqId) stream offsets
          | otherwise -> do
            m <- readIORef refThreads
            if IM.member reqId m
              then sendHeader $ ResponseError reqId $ MalformedRequest "duplicate request ID"
              else do
                sendHeader $ ResponseWait reqId
                -- Fork a thread to send a delayed response
                let finish _ = do
                      _ <- popThread reqId
                      removeActivity stream

                tid <- flip forkFinally finish $ do
                  addActivity stream
                  offsets' <- atomically $ do
                    (ready', offsets') <- query
                    check ready'
                    pure offsets'
                  send (ResponseDelayed reqId) stream offsets'
                writeIORef refThreads $! IM.insert reqId tid m

      `catch` \e -> sendHeader $ ResponseError reqId e
    Right (RawClean reqId) -> do
      tid <- popThread reqId
      mapM_ killThread tid
    Left err -> throwIO $ MalformedRequest err
  where
    popThread reqId = atomicModifyIORef' refThreads
      $ swap . IM.updateLookupWithKey (\_ _ -> Nothing) reqId

    sendHeader x = withMVar vConn $ \conn -> SB.sendAll conn $ encode x
    send header Stream{..} ((s0, p0), (s1, p1)) = withMVar vConn $ \conn -> do
      SB.sendAll conn $ encode (header, PayloadHeader s0 s1 p0 indexNames)
      -- byte offset + number of indices
      let siz = 8 * (length indexNames + 1)
      -- Send byte offsets and indices
      SF.sendFile' conn indexHandle (fromIntegral $ siz * succ s0) (fromIntegral $ siz * (s1 - s0))
      -- Send payloads
      SF.sendFile' conn payloadHandle (fromIntegral p0) (fromIntegral $ p1 - p0)

data Settings = Settings
  { reapInterval :: Double
  , streamLifetime :: Double
  , port :: S.PortNumber
  , livePrefix :: FilePath
  , archivePrefix :: Maybe FilePath
  }

startServer
    :: Settings
    -> IO ()
startServer Settings{..} = withFranzReader livePrefix $ \franzReader -> do

  hSetBuffering stderr LineBuffering
  _ <- forkIO $ reaper reapInterval streamLifetime franzReader

  vMounts <- newResourceMap

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
      buf <- newIORef B.empty
      let respondLoop path = do
            SB.sendAll conn apiVersion
            logServer [show connAddr, show path]
            ref <- newIORef IM.empty
            vConn <- newMVar conn
            forever (respond franzReader ref path buf vConn) `finally` do
              readIORef ref >>= mapM_ killThread

      forkFinally (runGetRecv buf conn get >>= \case
        Left _ -> throwIO $ MalformedRequest "Expecting a path"
        Right pathBS -> do
          let path = B.unpack pathBS
          case archivePrefix of
            -- just start a session without thinking about archives
            Nothing -> respondLoop path
            -- Mount a squashfs image and increment the counter
            Just prefix | src <- prefix </> path -> withSharedResource vMounts path
              (mountFuse src (livePrefix </> path))
              (\fuse -> do
                -- close the last client's streams
                streams <- atomically $ do
                  streams <- readTVar $ vStreams franzReader
                  writeTVar (vStreams franzReader) $ HM.delete path streams
                  pure streams
                forM_ (HM.lookup path streams) $ mapM_ closeStream
                killFuse fuse (livePrefix </> path) `finally` do
                  pid <- getPid fuse
                  forM_ pid $ \p -> logServer ["Undead squashfuse detected:", show p])
              (const $ respondLoop path)
        )
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encode $ ResponseError (-1) e
              Nothing -> logServer [show ex]
            Right _ -> return ()
          S.close conn
          logServer [show connAddr, "disconnected"]

logServer :: [String] -> IO ()
logServer = hPutStrLn stderr . unwords . (:) "[server]"

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
