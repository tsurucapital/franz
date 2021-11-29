{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RankNTypes #-}
module Database.Franz.Internal.Fuse where

import Control.Exception hiding (throw)
import Control.Monad (when)
import Control.Retry
import System.Directory
import System.Process (ProcessHandle, spawnProcess, cleanupProcess,
  waitForProcess, getProcessExitCode)

mountFuse :: ([String] -> IO ()) -- logger
  -> (forall x. String -> IO x) -- throw an exception
  -> FilePath -> FilePath -> IO ProcessHandle
mountFuse logger throw src dest = do
  createDirectoryIfMissing True dest
  logger ["squashfuse", "-f", src, dest]
  bracketOnError (spawnProcess "squashfuse" ["-f", src, dest]) (flip (killFuse logger) dest) $ \fuse -> do
    -- It keeps process handles so that mounted directories are cleaned up
    -- but there's no easy way to tell when squashfuse finished mounting.
    -- Wait until the destination becomes non-empty.
    notMounted <- retrying (limitRetries 5 <> exponentialBackoff 100000) (const pure)
      $ \status -> getProcessExitCode fuse >>= \case
        Nothing -> do
          logger ["Waiting for squashfuse to mount", src, ":", show status]
          null <$> listDirectory dest
        Just e -> do
          removeDirectory dest
          throw $ "squashfuse exited with " <> show e
    when notMounted $ throw $ "Failed to mount " <> src
    return fuse

killFuse :: ([String] -> IO ()) -> ProcessHandle -> FilePath -> IO ()
killFuse logger fuse path = do
  cleanupProcess (Nothing, Nothing, Nothing, fuse)
  e <- waitForProcess fuse
  logger ["squashfuse:", show e]
  removeDirectory path
