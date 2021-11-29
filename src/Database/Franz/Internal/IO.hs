{-# LANGUAGE RecordWildCards #-}
module Database.Franz.Internal.IO (getInt64le, runGetRecv, hGetRange) where

import Data.IORef
import Data.Serialize hiding (getInt64le)
import Data.Typeable (cast)
import Foreign.Ptr
import Foreign.ForeignPtr
import Foreign.C.Types
import GHC.IO.Exception
import GHC.IO.Handle.Internals (withHandle_)
import GHC.IO.Handle.Types (Handle__(..), Handle)
import Network.Socket as S
import Network.Socket.ByteString as SB
import System.Endian (fromLE64)
import System.IO.Error
import System.Posix.Types (Fd(..))
import qualified Data.ByteString as B
import qualified Data.ByteString.Internal as B
import qualified GHC.IO.FD as FD
import Data.Word (Word8)

-- | Better implementation of 'Data.Serialize.getInt64le'
getInt64le :: Num a => Get a
getInt64le = fromIntegral . fromLE64 <$> getWord64host
{-# INLINE getInt64le #-}

runGetRecv :: IORef B.ByteString -> S.Socket -> Get a -> IO (Either String a)
runGetRecv refBuf sock m = do
  lo <- readIORef refBuf
  let go (Done a lo') = do
        writeIORef refBuf lo'
        return $ Right a
      go (Partial cont) = SB.recv sock 4096 >>= go . cont
      go (Fail str lo') = do
        writeIORef refBuf lo'
        return $ Left $ show sock ++ str
  bs <- if B.null lo
    then SB.recv sock 4096
    else pure lo
  go $ runGetPartial m bs

withFd :: Handle -> (Fd -> IO a) -> IO a
withFd h f = withHandle_ "withFd" h $ \ Handle__{..} -> do
  case cast haDevice of
    Nothing -> ioError (ioeSetErrorString (mkIOError IllegalOperation
                                           "withFd" (Just h) Nothing)
                        "handle is not a file descriptor")
    Just fd -> f (Fd (fromIntegral (FD.fdFD fd)))

foreign import ccall safe "pread"
  c_pread :: Fd -> Ptr Word8 -> CSize -> CSize -> IO CSize

hGetRange :: Handle -> Int -> Int -> IO B.ByteString
hGetRange h len ofs = do
  fptr <- B.mallocByteString len
  count <- withFd h $ \fd -> withForeignPtr fptr $ \ptr -> c_pread fd ptr (fromIntegral len) (fromIntegral ofs)
  pure $ B.PS fptr 0 $ fromIntegral count