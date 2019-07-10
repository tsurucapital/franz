{-# LANGUAGE RecordWildCards #-}
module Database.Franz (
    -- * Writer interface
    WriterHandle,
    openWriter,
    closeWriter,
    withWriter,
    write
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as B
import Data.Foldable (toList)
import Data.Int
import System.Directory
import System.FilePath
import System.IO

data WriterHandle f = WriterHandle
  { hPayload :: Handle
  , hOffset :: Handle
  , hIndices :: f Handle
  , vOffset :: MVar Int64
  }

openWriter :: (Traversable f, Applicative f)
  => f String
  -- ^ index names: a fixed-length collection of the names of indices for this stream.
  -- Use Proxy if you don't want any indices. If you want only one type of index, use `Identity ""`.
  -> FilePath
  -> IO (WriterHandle f)
openWriter idents path = do
  createDirectoryIfMissing True path
  let payloadPath = path </> "payloads"
  let offsetPath = path </> "offsets"
  let indexPath = path </> "indices"
  alreadyExists <- doesFileExist payloadPath
  vOffset <- if alreadyExists
    then withFile payloadPath ReadMode hFileSize >>= newMVar . fromIntegral
    else newMVar 0
  writeFile indexPath $ unlines $ toList idents
  hPayload <- openFile payloadPath AppendMode
  hOffset <- openFile offsetPath AppendMode
  liftIO $ hSetBuffering hOffset NoBuffering
  hIndices <- forM idents $ \s -> do
    h <- openFile (indexPath ++ "." ++ s) AppendMode
    hSetBuffering h NoBuffering
    return h
  return WriterHandle{..}

closeWriter :: Foldable f => WriterHandle f -> IO ()
closeWriter WriterHandle{..} = do
  hClose hPayload
  hClose hOffset
  mapM_ hClose hIndices

withWriter :: (Traversable f, Applicative f) => f String -> FilePath -> (WriterHandle f -> IO a) -> IO a
withWriter idents path = bracket (openWriter idents path) closeWriter

write :: (Traversable f, Applicative f) => WriterHandle f
  -> f Int64 -- ^ index values
  -> B.ByteString -- ^ payload
  -> IO ()
write WriterHandle{..} ixs bs = modifyMVar_ vOffset $ \ofs -> do
  let len = fromIntegral (B.length bs)
  let ofs' = ofs + len + 8 * fromIntegral (length ixs + 1)
  BB.hPutBuilder hPayload $ BB.int64LE len
    <> foldMap BB.int64LE ixs
    <> BB.byteString bs
  sequence_ $ liftA2 (\h -> BB.hPutBuilder h . BB.int64LE) hIndices ixs
  hFlush hPayload
  BB.hPutBuilder hOffset $! BB.int64LE ofs'
  return ofs'
