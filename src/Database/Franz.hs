{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE BangPatterns #-}
module Database.Franz (
    -- * Writer interface
    WriterHandle,
    openWriter,
    closeWriter,
    withWriter,
    write,
    writeMany
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as B
import Data.Foldable (toList)
import Data.Int
import Data.IORef
import System.Directory
import System.FilePath
import System.IO

data WriterHandle f = WriterHandle
  { hPayload :: Handle
  , hOffset :: Handle
  , hIndices :: f Handle
  , vOffset :: MVar (Int, Int64)
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
    then do
      count <- withFile offsetPath ReadMode hFileSize
      size <- withFile payloadPath ReadMode hFileSize
      newMVar (fromIntegral count `div` 8, fromIntegral size)
    else newMVar (0,0)
  writeFile indexPath $ unlines $ toList idents
  hPayload <- openFile payloadPath AppendMode
  hOffset <- openFile offsetPath AppendMode
  hIndices <- forM idents $ \s -> openFile (indexPath ++ "." ++ s) AppendMode
  return WriterHandle{..}

closeWriter :: Foldable f => WriterHandle f -> IO ()
closeWriter WriterHandle{..} = do
  hClose hPayload
  hClose hOffset
  mapM_ hClose hIndices

withWriter :: (Traversable f, Applicative f) => f String -> FilePath -> (WriterHandle f -> IO a) -> IO a
withWriter idents path = bracket (openWriter idents path) closeWriter

write :: (Foldable f, Applicative f) => WriterHandle f
  -> f Int64 -- ^ index values
  -> B.ByteString -- ^ payload
  -> IO Int
write h ixs bs = writeMany h $ \f -> f ixs bs
{-# INLINE write #-}

-- | Write zero or more items and flush the change. The writing function must be
-- called from one thread.
writeMany :: (Foldable f, Applicative f) => WriterHandle f
  -> ((f Int64 -- ^ index values
    -> B.ByteString -- ^ payload
    -> IO Int -- ^ sequential number
    ) -> IO r)
  -> IO r
writeMany WriterHandle{..} cont = do

  vOffsets <- newIORef mempty

  let step ixs bs = modifyMVar vOffset $ \(n, ofs) -> do
        let len = fromIntegral (B.length bs)
        let ofs' = ofs + len + 8 * fromIntegral (length ixs + 1)
        BB.hPutBuilder hPayload $ BB.int64LE len
          <> foldMap BB.int64LE ixs
          <> BB.byteString bs
        sequence_ $ liftA2 (\h -> BB.hPutBuilder h . BB.int64LE) hIndices ixs
        modifyIORef' vOffsets (<>BB.int64LE ofs')
        let !n' = n + 1
        return ((n', ofs'), n)

  cont step `finally` do
    -- NB it's important to write the payload and indices prior to offsets
    -- because the reader watches the offset file then consume other files.
    -- Because of this, offsets are buffered in an IORef to prevent them from
    -- getting partially written.
    hFlush hPayload
    mapM_ hFlush hIndices
    readIORef vOffsets >>= BB.hPutBuilder hOffset
    hFlush hOffset
