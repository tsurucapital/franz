{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
import Control.DeepSeq (NFData(..))
import Control.Exception
import Control.Monad
import qualified Data.ByteString.FastBuilder as BB
import qualified Data.Vector.Storable.Mutable as MV
import Data.Foldable (toList)
import Data.Int
import Data.Word (Word64)
import Data.IORef
import Data.Kind (Type)
import Foreign.Ptr (Ptr)
import Foreign.Storable (Storable(..))
import GHC.IO.Handle.FD (openFileBlocking)
import System.Directory
import System.Endian (toLE64)
import System.FilePath
import System.IO

data WriterHandle (f :: Type -> Type) = WriterHandle
  { hPayload :: !Handle
  , hOffset :: !Handle -- ^ Handle for offsets and indices
  , vOffset :: !(MVar (Int, Word64)) -- ^ (next sequential number, current payload file size)
  , offsetBuf :: !(MV.IOVector Word64) -- ^ pending indices
  , offsetPtr :: !(IORef Int) -- ^ the number of pending indices
  , indexCount :: !Int -- ^ the number of Word64s to write for item
  }
instance NFData (WriterHandle f) where
  rnf WriterHandle{} = ()

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
  -- Open the file in blocking mode because a write on a non-blocking
  -- FD makes use of an unsafe call to write(2), which in turn blocks other
  -- threads when GC runs.
  hPayload <- openFileBlocking payloadPath AppendMode
  hOffset <- openFileBlocking offsetPath AppendMode
  offsetBuf <- MV.new offsetBufferSize
  offsetPtr <- newIORef 0
  let indexCount = length idents + 1
  return WriterHandle{..}

-- | Flush any pending data and close a 'WriterHandle'.
closeWriter :: WriterHandle f -> IO ()
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
  pos <- readIORef offsetPtr
  pos' <- if pos + indexCount >= offsetBufferSize
    then 0 <$ unsafeFlush h
    else return pos
  MV.write offsetBuf pos' $ toLE64 $ fromIntegral ofs'
  forM_ (zip [pos'+1..] (toList ixs))
    $ \(i, v) -> MV.write offsetBuf i $ toLE64 $ fromIntegral v
  writeIORef offsetPtr (pos' + indexCount)

  let !n' = n + 1
  return ((n', ofs'), n)
{-# INLINE write #-}

-- | Flush the changes.
flush :: WriterHandle f -> IO ()
flush h = withMVar (vOffset h) $ const $ unsafeFlush h

-- | Flush the change without locking 'vOffset'
unsafeFlush :: WriterHandle f -> IO ()
unsafeFlush WriterHandle{..}  = do
  -- NB it's important to write the payload and indices prior to offsets
  -- because the reader watches the offset file then consume other files.
  -- Because of this, offsets are buffered in a buffer.
  len <- readIORef offsetPtr
  when (len > 0) $ do
    hFlush hPayload
    MV.unsafeWith offsetBuf $ \ptr -> hPutElems hOffset ptr len
    writeIORef offsetPtr 0
    hFlush hOffset

-- | Same as hPutBuf but with a number of elements to write instead of a number
-- of bytes.
hPutElems :: forall a. Storable a => Handle -> Ptr a -> Int -> IO ()
hPutElems hdl ptr len = hPutBuf hdl ptr (len * sizeOf (undefined :: a))
{-# INLINE hPutElems #-}
