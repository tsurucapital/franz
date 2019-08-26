{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE KindSignatures #-}
module Database.Franz (
    -- * Writer interface
    WriterHandle,
    openWriter,
    closeWriter,
    withWriter,
    write,
    flush,
    getLastSeqNo
    ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import qualified Data.ByteString.FastBuilder as BB
import qualified Data.Vector.Storable.Mutable as MV
import Data.Foldable (toList)
import Data.Int
import Data.Word (Word64)
import Data.IORef
import Data.Kind (Type)
import GHC.IO.Handle.FD (openFileBlocking)
import System.Directory
import System.Endian (toLE64)
import System.FilePath
import System.IO

data WriterHandle (f :: Type -> Type) = WriterHandle
  { hPayload :: Handle
  , hOffset :: Handle
  , vOffset :: MVar (Int, Word64)
  , offsetBuf :: MV.IOVector Word64
  , offsetPtr :: IORef Int
  , indexCount :: Int
  }

-- | Get the sequential number of the last item item written.
getLastSeqNo :: WriterHandle f -> IO Int
getLastSeqNo = fmap (subtract 1 . fst) . readMVar . vOffset

openWriter :: Foldable f
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
    then do
      count <- withFile offsetPath ReadMode hFileSize
      size <- withFile payloadPath ReadMode hFileSize
      newMVar (fromIntegral count `div` 8, fromIntegral size)
    else newMVar (0,0)
  writeFile indexPath $ unlines $ toList idents
  hPayload <- openFileBlocking payloadPath AppendMode
  hOffset <- openFileBlocking offsetPath AppendMode
  offsetBuf <- MV.new offsetBufferSize
  offsetPtr <- newIORef 0
  let indexCount = length idents + 1
  return WriterHandle{..}

closeWriter :: Foldable f => WriterHandle f -> IO ()
closeWriter h@WriterHandle{..} = do
  flush h
  hClose hPayload
  hClose hOffset

withWriter :: Foldable f => f String -> FilePath -> (WriterHandle f -> IO a) -> IO a
withWriter idents path = bracket (openWriter idents path) closeWriter

offsetBufferSize :: Int
offsetBufferSize = 256

write :: Foldable f
  => WriterHandle f
  -> f Int64 -- ^ index values
  -> BB.Builder -- ^ payload
  -> IO Int
write h@WriterHandle{..} ixs !bs = modifyMVar vOffset $ \(n, ofs) -> do
  len <- fromIntegral <$> BB.hPutBuilderLen hPayload bs
  let ofs' = ofs + len

  let go :: Int -> IO ()
      go i0 = do
        MV.write offsetBuf i0 $ toLE64 $ fromIntegral ofs'
        forM_ (zip [i0+1..] (toList ixs))
          $ \(i, v) -> MV.write offsetBuf i $ toLE64 $ fromIntegral v

  pos <- readIORef offsetPtr
  if pos + indexCount >= offsetBufferSize
    then do
      unsafeFlush h
      go 0
      writeIORef offsetPtr indexCount
    else do
      go pos
      writeIORef offsetPtr (pos + indexCount)
  let !n' = n + 1
  return ((n', ofs'), n)
{-# INLINE write #-}

-- | Flush the changes.
flush :: WriterHandle f -> IO ()
flush h = withMVar (vOffset h) $ const $ unsafeFlush h

unsafeFlush :: WriterHandle f -> IO ()
unsafeFlush WriterHandle{..}  = do
  -- NB it's important to write the payload and indices prior to offsets
  -- because the reader watches the offset file then consume other files.
  -- Because of this, offsets are buffered in a buffer.
  len <- readIORef offsetPtr
  when (len > 0) $ do
    hFlush hPayload
    MV.unsafeWith offsetBuf $ \ptr -> hPutBuf hOffset ptr (len * 8)
    writeIORef offsetPtr 0
    hFlush hOffset
