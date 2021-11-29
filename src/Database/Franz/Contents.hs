{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Franz.Contents
  ( Contents
  , Database.Franz.Internal.Contents.indexNames
  , Item(..)
  , toList
  , last
  , length
  , index
  , lookupIndex
  ) where

import qualified Data.ByteString.Char8 as B
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import Database.Franz.Internal.Protocol
import Database.Franz.Internal.Contents
import Data.Int
import Prelude hiding (length, last)

data Item = Item
  { seqNo :: !Int
  , indices :: !(U.Vector Int64)
  , payload :: !B.ByteString
  } deriving (Show, Eq)

toList :: Contents -> [Item]
toList contents = [unsafeIndex contents i | i <- [0..length contents - 1]]

last :: Contents -> Maybe Item
last contents
  | i >= 0 = Just $ unsafeIndex contents i
  | otherwise = Nothing
  where
    i = length contents - 1

index :: Contents -> Int -> Maybe Item
index contents i
  | i >= length contents || i < 0 = Nothing
  | otherwise = Just $ unsafeIndex contents i

unsafeIndex :: Contents -> Int -> Item
unsafeIndex Contents{..} i = Item{..}
  where
    ofs0 = maybe payloadOffset fst $ indicess V.!? (i - 1)
    (ofs1, indices) = indicess V.! i
    seqNo = seqnoOffset + i + 1
    payload = B.take (ofs1 - payloadOffset) $ B.drop (ofs0 - payloadOffset) payloads

lookupIndex :: Contents -> IndexName -> Maybe (Item -> Int64)
lookupIndex Contents{indexNames} name
  = (\j Item{indices} -> indices U.! j) <$> V.elemIndex name indexNames
