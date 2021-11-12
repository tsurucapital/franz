{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Franz.Contents
  ( SomeIndexMap
  , Contents
  , getResponse
  , readContents
  ) where

import Data.Serialize hiding (getInt64le)
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector as V
import Database.Franz.Internal
import Database.Franz.Protocol
import Database.Franz.Reader
import Data.Int

type SomeIndexMap = HM.HashMap IndexName Int64

-- | (seqno, indices, payloads)
type Contents = V.Vector (Int, SomeIndexMap, B.ByteString)

-- A vector containing file offsets and extra indices
type IndexVec = V.Vector (Int, [Int64])

getIndexVec :: [IndexName] -> Int -> Get IndexVec
getIndexVec names len = V.replicateM len $ (,) <$> getInt64le <*> traverse (const getInt64le) names

getResponse :: Get Contents
getResponse = do
  PayloadHeader s0 s1 p0 names <- get
  let df = s1 - s0
  if df <= 0
    then pure mempty
    else do
      ixs <- getIndexVec names df
      payload <- getByteString $ fst (V.unsafeLast ixs) - p0
      pure $! sliceContents names p0 s0 ixs payload

sliceContents :: [IndexName]
  -> Int -- first file offset
  -> Int -- first seqno
  -> IndexVec -> B.ByteString -> Contents
sliceContents names p0 s0 ixs payload = V.create $ do
  vres <- VGM.unsafeNew (V.length ixs)
  let go i ofs0
        | i >= V.length ixs = pure ()
        | otherwise = do
          let (ofs1, indices) = V.unsafeIndex ixs i
              !m = HM.fromList $ zip names indices
              !bs = B.take (ofs1 - ofs0) $ B.drop (ofs0 - p0) payload
              !num = s0 + i + 1
          VGM.unsafeWrite vres i (num, m, bs)
          go (i + 1) ofs1
  go 0 p0
  return vres

readContents :: Stream -> QueryResult -> IO Contents
readContents Stream{..} ((s0, p0), (s1, p1)) = do
  -- byte offset + number of indices
  let siz = 8 * (length indexNames + 1)
  indexBS <- hGetRange indexHandle (siz * (s1 - s0)) (siz * succ s0)
  payload <- hGetRange payloadHandle (p1 - p0) p0
  let indexV = either error id $ runGet (getIndexVec indexNames (s1 - s0)) indexBS
  pure $! sliceContents indexNames p0 s0 indexV payload
