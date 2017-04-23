{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Types where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int

data ConsumerRequest = Blocking | NonBlocking | Seek !Int64

instance Binary ConsumerRequest where
  get = getWord8 >>= \case
    66{-B-} -> return Blocking
    78{-N-} -> return NonBlocking
    83{-S-} -> Seek <$> getInt64le
    _ -> fail "Unknown tag"
  put Blocking = putWord8 66
  put NonBlocking = putWord8 78
  put (Seek b) = putWord8 83 >> putInt64le b

data ProducerRequest = Write !Int64
  | WriteSeqNo

instance Binary ProducerRequest where
  get = getWord8 >>= \case
    87{-W-} -> Write <$> getInt64le
    83{-S-} -> pure WriteSeqNo
    _ -> fail "Unknown tag"
  put (Write ofs) = putWord8 87 >> putInt64le ofs
  put WriteSeqNo = putWord8 83
