{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Franz.Internal.Contents
    ( IndexVec
    , Contents(..)
    , getResponse
    , readContents
    )
    where

import Prelude hiding (length)
import Data.Serialize hiding (getInt64le)
import qualified Data.ByteString.Char8 as B
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as U
import Database.Franz.Internal.Reader
import Database.Franz.Internal.Protocol
import Database.Franz.Internal.IO
import Data.Int

-- A vector containing file offsets and extra indices
type IndexVec = V.Vector (Int, U.Vector Int64)

data Contents = Contents
  { indexNames :: !(V.Vector IndexName)
  , payloads :: !B.ByteString
  , indicess :: !IndexVec
  , length :: !Int
  , payloadOffset :: !Int
  , seqnoOffset :: !Int
  }

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

readContents :: Stream -> QueryResult -> IO Contents
readContents Stream{indexNames, payloadHandle, indexHandle} ((seqnoOffset, payloadOffset), (s1, p1)) = do
  let length = s1 - seqnoOffset
  -- byte offset + number of indices
  let indexSize = 8 * (V.length indexNames + 1)
  indexBS <- hGetRange indexHandle (indexSize * length) (indexSize * succ seqnoOffset)
  payloads <- hGetRange payloadHandle (p1 - payloadOffset) payloadOffset
  let indicess = either error id $ runGet (getIndexVec indexNames length) indexBS
  pure Contents{..}