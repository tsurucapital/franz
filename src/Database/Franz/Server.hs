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
import Control.Concurrent.STM
import Database.Franz.Internal
import Database.Franz.Protocol
import Database.Franz.Reader
import Data.Serialize
import qualified Data.IntMap.Strict as IM
import Data.IORef
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.Directory
import System.FilePath
import System.IO
import System.Process (ProcessHandle, readProcess, spawnProcess, terminateProcess, waitForProcess)

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
                let finish _ = do
                      _ <- popThread reqId
                      removeActivity stream

                tid <- flip forkFinally finish
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
      tid <- popThread reqId
      mapM_ killThread tid
    Left err -> throwIO $ MalformedRequest err
  where
    popThread reqId = atomicModifyIORef' refThreads (\m -> (IM.delete reqId m, IM.lookup reqId m))

    sendHeader x = withMVar vConn $ \conn -> SB.sendAll conn $ encode x
    send header Stream{..} ((s0, p0), (s1, p1)) = withMVar vConn $ \conn -> do
      SB.sendAll conn $ encode (header, PayloadHeader s0 s1 p0 indexNames)
      let siz = 8 * (length indexNames + 1)
      SF.sendFile' conn indexHandle (fromIntegral $ siz * succ s0) (fromIntegral $ siz * (s1 - s0))
      SF.sendFile' conn payloadHandle (fromIntegral p0) (fromIntegral $ p1 - p0)

data Env = Env
  { vMountCount :: TVar (HM.HashMap FilePath (ProcessHandle, Int))
  , franzReader :: FranzReader
  }

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
startServer settings@Settings{..} = withFranzReader livePrefix $ \franzReader -> do

  hSetBuffering stderr LineBuffering
  _ <- forkIO $ reaper reapInterval streamLifetime franzReader

  vMountCount <- newTVarIO HM.empty
  let env = Env{..}
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
            forever (respond franzReader ref path buf vConn) `finally` do
              readIORef ref >>= mapM_ killThread

      forkFinally (do
        decode <$> SB.recv conn 4096 >>= \case
          Left _ -> throwIO $ MalformedRequest "Expecting a path"
          Right pathBS -> do
            let path = B.unpack pathBS
            -- Mount a squashfs image and increment the counter
            initialise settings env path
            respondLoop path `finally` cleanup settings env connAddr path
        )
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encode $ ResponseError (-1) e
              Nothing -> logServer [show ex]
            Right _ -> return ()
          S.close conn

logServer :: [String] -> IO ()
logServer = hPutStrLn stderr . unwords . (:) "[server]"

callProcess' :: String -> [String] -> IO ()
callProcess' exe args = do
  logServer $ exe : args
  out <- readProcess exe args ""
  forM_ (lines out) $ logServer . pure

initialise :: Settings -> Env -> FilePath -> IO ()
initialise Settings{..} Env{..} path = join $ atomically $ do
  m <- readTVar vMountCount
  case HM.lookup path m of
    Nothing -> return $ do
      let dest = livePrefix </> path
      forM_ ((</>path) <$> archivePrefix) $ \src -> do
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

cleanup :: Settings -> Env -> S.SockAddr -> String -> IO ()
cleanup Settings{..} Env{..} connAddr path = do
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
            ss <- readTVar $ vStreams franzReader
            writeTVar (vStreams franzReader) $ HM.delete path ss
            return ss
          forM_ (HM.lookup path streams) $ mapM_ closeStream
          terminateProcess fuse
          _ <- waitForProcess fuse
          callProcess' "rmdir" [livePrefix </> path]
          atomically $ writeTVar vMountCount $ HM.delete path m
      Just (fuse, n) -> do
        writeTVar vMountCount $! HM.insert path (fuse, n - 1) m
        pure (pure ())
      Nothing -> pure (pure ())
