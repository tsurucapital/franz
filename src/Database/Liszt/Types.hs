{-# LANGUAGE LambdaCase #-}
module Database.Liszt.Types where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int

data ConsumerRequest = Blocking | Seek !Int64

instance Binary ConsumerRequest where
  get = getWord8 >>= \case
    66 -> return Blocking
    83 -> Seek <$> getInt64le
    _ -> fail "Unknown tag"
  put Blocking = putWord8 66
  put (Seek b) = putWord8 83 >> putInt64le b

data ProducerRequest = Write !Int64
  | Sequential

instance Binary ProducerRequest where
  get = getWord8 >>= \case
    87 -> Write <$> getInt64le
    83 -> pure Sequential
    _ -> fail "Unknown tag"
  put (Write ofs) = putWord8 87 >> putInt64le ofs
  put Sequential = putWord8 83
