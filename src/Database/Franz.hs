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
    writeMany,
    getLastSeqNo
    ) where

import Control.Concurrent
import Control.Exception
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as B
import Data.Foldable (toList)
import Data.Int
import Data.IORef
import Data.Kind (Type)
import System.Directory
import System.FilePath
import System.IO

data WriterHandle (f :: Type -> Type) = WriterHandle
  { hPayload :: Handle
  , hOffset :: Handle
  , vOffset :: MVar (Int, Int64)
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
  hPayload <- openFile payloadPath AppendMode
  hOffset <- openFile offsetPath AppendMode
  return WriterHandle{..}

closeWriter :: Foldable f => WriterHandle f -> IO ()
closeWriter WriterHandle{..} = do
  hClose hPayload
  hClose hOffset

withWriter :: Foldable f => f String -> FilePath -> (WriterHandle f -> IO a) -> IO a
withWriter idents path = bracket (openWriter idents path) closeWriter

write :: Foldable f => WriterHandle f
  -> f Int64 -- ^ index values
  -> B.ByteString -- ^ payload
  -> IO Int
write h ixs !bs = writeMany h $ \f -> f ixs bs
{-# INLINE write #-}

-- | Write zero or more items and flush the change. The writing function must be
-- called from one thread.
writeMany :: Foldable f => WriterHandle f
  -> ((f Int64  -> B.ByteString -> IO Int) -> IO r)
  -- ^ index values -> payload -> sequential number
  -> IO r
writeMany WriterHandle{..} cont = do

  vOffsets <- newIORef mempty

  let step ixs !bs = modifyMVar vOffset $ \(n, ofs) -> do
        let len = fromIntegral (B.length bs)
        let ofs' = ofs + len
        B.hPutStr hPayload bs
        let ibody = BB.int64LE ofs' <> foldMap BB.int64LE ixs
        modifyIORef' vOffsets (<>ibody)
        let !n' = n + 1
        return ((n', ofs'), n)

  cont step `finally` do
    -- NB it's important to write the payload and indices prior to offsets
    -- because the reader watches the offset file then consume other files.
    -- Because of this, offsets are buffered in an IORef to prevent them from
    -- getting partially written.
    hFlush hPayload
    readIORef vOffsets >>= BB.hPutBuilder hOffset
    hFlush hOffset
