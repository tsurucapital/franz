module Database.Franz.Writer.Simple (
    -- * Writer interface
    Franz.WriterHandle,
    openWriter,
    Franz.closeWriter,
    withWriter,
    write,
    Franz.flush,
    Franz.getLastSeqNo,
    ToFastBuilder(..)
    ) where

import Data.Proxy
import qualified Database.Franz.Writer as Franz
import qualified Data.ByteString.FastBuilder as BB
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

openWriter :: FilePath -> IO (Franz.WriterHandle Proxy)
openWriter = Franz.openWriter Proxy

withWriter :: FilePath -> (Franz.WriterHandle Proxy -> IO a) -> IO a
withWriter = Franz.withWriter Proxy

class ToFastBuilder a where
    toFastBuilder :: a -> BB.Builder

instance ToFastBuilder B.ByteString where
    toFastBuilder = BB.byteString

instance ToFastBuilder BL.ByteString where
    toFastBuilder = foldMap BB.byteString . BL.toChunks

instance ToFastBuilder BB.Builder where
    toFastBuilder = id

write :: Franz.WriterHandle Proxy
  -> BB.Builder
  -> IO Int
write h = Franz.write h Proxy . toFastBuilder
{-# INLINE write #-}