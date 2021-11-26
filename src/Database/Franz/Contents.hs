{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Franz.Contents
  ( Contents
  , Database.Franz.Contents.indexNames
  , Item(..)
  , toList
  , last
  , length
  , index
  , lookupIndex
  -- * Internal
  , getResponse
  , readContents
  ) where

import Data.Serialize hiding (getInt64le)
import qualified Data.ByteString.Char8 as B
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import Database.Franz.Internal
import Database.Franz.Protocol
import Database.Franz.Reader
import Data.Int
import Prelude hiding (length, last)

data Item = Item
  { seqNo :: !Int
  , indices :: !(U.Vector Int64)
  , payload :: !B.ByteString
  } deriving (Show, Eq)

data Contents = Contents
  { indexNames :: !(V.Vector IndexName)
  , payloads :: !B.ByteString
  , indicess :: !IndexVec
  , length :: !Int
  , payloadOffset :: !Int
  , seqnoOffset :: !Int
  }

toList :: Contents -> [Item]
toList contents = [unsafeIndex contents i | i <- [0..length contents - 1]]

-- A vector containing file offsets and extra indices
type IndexVec = V.Vector (Int, U.Vector Int64)

getIndexVec :: V.Vector IndexName -> Int -> Get IndexVec
getIndexVec names len = V.replicateM len
  $ (,) <$> getInt64le <*> U.convert `fmap` traverse (const getInt64le) names

getResponse :: Get Contents
getResponse = do
  PayloadHeader seqnoOffset s1 payloadOffset indexNames <- get
  let length = s1 - seqnoOffset
  if length <= 0
    then pure Contents{ payloads = B.empty, indicess = V.empty, ..}
    else do
      indicess <- getIndexVec indexNames length
      payloads <- getByteString $ fst (V.unsafeLast indicess) - payloadOffset
      pure Contents{..}

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

readContents :: Stream -> QueryResult -> IO Contents
readContents Stream{indexNames, payloadHandle, indexHandle} ((seqnoOffset, payloadOffset), (s1, p1)) = do
  let length = s1 - seqnoOffset
  -- byte offset + number of indices
  let indexSize = 8 * (V.length indexNames + 1)
  indexBS <- hGetRange indexHandle (indexSize * length) (indexSize * succ seqnoOffset)
  payloads <- hGetRange payloadHandle (p1 - payloadOffset) payloadOffset
  let indicess = either error id $ runGet (getIndexVec indexNames length) indexBS
  pure Contents{..}