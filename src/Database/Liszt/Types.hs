{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Types where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int

data ConsumerRequest = Read
  | Seek !Int64
  | NonBlocking ConsumerRequest deriving Show

instance Binary ConsumerRequest where
  get = getWord8 >>= \case
    78{-N-} -> NonBlocking <$> get
    82{-R-} -> pure Read
    83{-S-} -> Seek <$> getInt64le
    _ -> fail "Unknown tag"
  put Read = putWord8 82
  put (Seek b) = putWord8 83 >> putInt64le b
  put (NonBlocking r) = putWord8 78 >> put r

data ProducerRequest = Write !Int64
  | WriteSeqNo
  deriving Show

instance Binary ProducerRequest where
  get = getWord8 >>= \case
    87{-W-} -> Write <$> getInt64le
    83{-S-} -> pure WriteSeqNo
    _ -> fail "Unknown tag"
  put (Write ofs) = putWord8 87 >> putInt64le ofs
  put WriteSeqNo = putWord8 83
