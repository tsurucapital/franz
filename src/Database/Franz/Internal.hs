module Database.Franz.Internal (getInt64le, runGetRecv) where

import qualified Data.ByteString as B
import Data.IORef
import Data.Serialize hiding (getInt64le)
import Network.Socket as S
import Network.Socket.ByteString as SB
import System.Endian (fromLE64)

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
