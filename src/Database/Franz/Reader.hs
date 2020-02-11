{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
module Database.Franz.Reader where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Serialize
import Database.Franz.Protocol
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as IM
import qualified Data.Vector.Unboxed as U
import qualified Data.Vector as V
import Data.Void
import Data.Maybe (isJust)
import GHC.Clock (getMonotonicTime)
import System.Directory
import System.FilePath
import System.IO
import System.FSNotify

data Stream = Stream
  { vOffsets :: !(TVar (IM.IntMap Int))
  , indexNames :: ![B.ByteString]
  , indices :: !(HM.HashMap B.ByteString (TVar (IM.IntMap Int)))
  , vCount :: !(TVar Int)
  , vCaughtUp :: !(TVar Bool)
  , followThread :: !ThreadId
  , indexHandle :: !Handle
  , payloadHandle :: !Handle
  , vActivity :: !(TVar Activity)
  }

type Activity = Either Double Int

addActivity :: Activity -> Activity
addActivity (Left _) = Right 0
addActivity (Right n) = Right (n + 1)

removeActivity :: Stream -> IO ()
removeActivity str = do
  now <- getMonotonicTime
  atomically $ modifyTVar' (vActivity str) $ \case
    Left _ -> Left now
    Right n
      | n <= 0 -> Left now
      | otherwise -> Right (n - 1)

closeStream :: Stream -> IO ()
closeStream Stream{..} = do
  killThread followThread
  hClose payloadHandle
  hClose indexHandle

createStream :: WatchManager -> FilePath -> IO Stream
createStream man path = do
  let offsetPath = path </> "offsets"
  let payloadPath = path </> "payloads"
  exist <- doesFileExist offsetPath
  unless exist $ throwIO $ StreamNotFound offsetPath
  initialOffsetsBS <- B.readFile offsetPath
  payloadHandle <- openBinaryFile payloadPath ReadMode
  indexNames <- B.lines <$> B.readFile (path </> "indices")
  let icount = 1 + length indexNames
  let count = B.length initialOffsetsBS `div` (8 * icount)
  let getI = fromIntegral <$> getInt64le
  initialIndices <- either (throwIO . InternalError) pure
    $ runGet (V.replicateM count $ U.replicateM icount getI) initialOffsetsBS
  let initialOffsets = IM.fromList $ V.toList
        $ V.zip (V.enumFromN 0 count) $ V.map U.head initialIndices
  vOffsets <- newTVarIO $! initialOffsets
  vCaughtUp <- newTVarIO False
  vCount <- newTVarIO $! IM.size initialOffsets
  _ <- watchDir man path (\case
    Modified p _ _ | p == offsetPath -> True
    _ -> False)
    $ const $ atomically $ writeTVar vCaughtUp False

  vIndices <- forM [1..length indexNames] $ \i -> newTVarIO
    $ IM.fromList $ V.toList $ V.zip (V.map (U.! i) initialIndices) (V.enumFromN 0 count)

  indexHandle <- openFile offsetPath ReadMode

  let final :: Either SomeException Void -> IO ()
      final (Left exc) | Just ThreadKilled <- fromException exc = pure ()
      final (Left exc) = logFollower [path, "terminated with", show exc]
      final (Right v) = absurd v

  -- TODO broadcast an exception if it exits?
  followThread <- flip forkFinally final $ do
    forM_ (IM.maxViewWithKey initialOffsets) $ \((i, _), _) -> do
      hSeek indexHandle AbsoluteSeek $ fromIntegral $ succ i * icount * 8
    forever $ do
      bs <- B.hGet indexHandle (8 * icount)
      if B.null bs
        then do
          atomically $ writeTVar vCaughtUp True
          atomically $ readTVar vCaughtUp >>= check . not
        else do
          ofs : indices <- either (throwIO . InternalError) pure $ runGet (replicateM icount getI) bs
          atomically $ do
            i <- readTVar vCount
            modifyTVar' vOffsets $ IM.insert i ofs
            forM_ (zip vIndices indices) $ \(v, x) -> modifyTVar' v $ IM.insert (fromIntegral x) i
            writeTVar vCount $! i + 1

  let indices = HM.fromList $ zip indexNames vIndices

  vActivity <- getMonotonicTime >>= newTVarIO . Left
  return Stream{..}
  where
    logFollower = hPutStrLn stderr . unwords . (:) "[follower]"

type QueryResult = ((Int, Int) -- starting SeqNo, byte offset
    , (Int, Int)) -- ending SeqNo, byte offset

range :: Int -- ^ from
  -> Int -- ^ to
  -> RequestType
  -> IM.IntMap Int -- ^ offsets
  -> (Bool, QueryResult)
range begin end rt allOffsets = case rt of
    AllItems -> (ready, (first, maybe first fst $ IM.maxViewWithKey body))
    LastItem -> case IM.maxViewWithKey body of
      Nothing -> (False, (zero, zero))
      Just (ofs', r) -> case IM.maxViewWithKey (IM.union left r) of
        Just (ofs, _) -> (ready, (ofs, ofs'))
        Nothing -> (ready, (zero, ofs'))
  where
    zero = (-1, 0)
    ready = isJust lastItem || not (null cont)
    (wing, lastItem, cont) = IM.splitLookup end allOffsets
    (left, body) = splitR begin $ maybe id (IM.insert end) lastItem wing
    first = maybe zero fst $ IM.maxViewWithKey left

splitR :: Int -> IM.IntMap a -> (IM.IntMap a, IM.IntMap a)
splitR i m = let (l, p, r) = IM.splitLookup i m in (l, maybe id (IM.insert i) p r)

data FranzReader = FranzReader
  { watchManager :: WatchManager
  , vStreams :: TVar (HM.HashMap FilePath (HM.HashMap B.ByteString Stream))
  , prefix :: FilePath
  }

reaper :: Double -- interval
  -> Double -- lifetime
  -> FranzReader -> IO ()
reaper int life FranzReader{..} = forever $ do
  now <- getMonotonicTime
  (count, xs) <- atomically $ do
    list <- newTVar []
    m <- readTVar vStreams
    m' <- forM m $ \g -> fmap (HM.mapMaybe id)
      $ forM g $ \s -> readTVar (vActivity s) >>= \case
        Left t | now - t >= life -> Nothing <$ modifyTVar list (s:)
        _ -> pure $ Just s
    writeTVar vStreams $! HM.filter (not . null) m'
    (,) (sum $ fmap HM.size m) <$> readTVar list
  mapM_ closeStream xs
  unless (null xs) $ hPutStrLn stderr $ unwords
    [ "[reaper] closed"
    , show (length xs)
    , "out of"
    , show count
    ]
  threadDelay $ floor $ int * 1e6

withFranzReader :: FilePath -> (FranzReader -> IO ()) -> IO ()
withFranzReader prefix k = do
  vStreams <- newTVarIO HM.empty
  withManager $ \watchManager -> k FranzReader{..}

handleQuery :: FranzReader
  -> FilePath
  -> Query
  -> IO (Stream, STM (Bool, QueryResult))
handleQuery FranzReader{..} dir (Query name begin_ end_ rt) = do
  allStreams <- readTVarIO vStreams
  let path = prefix </> dir </> B.unpack name
  let streams = maybe mempty id $ HM.lookup dir allStreams
  stream@Stream{..} <- case HM.lookup name streams of
    Nothing -> do
      s <- createStream watchManager path
      atomically $ modifyTVar' vStreams $ HM.insert dir $ HM.insert name s streams
      return s
    Just vStream -> return vStream
  atomically $ modifyTVar' vActivity addActivity
  return $ (,) stream $ do
    readTVar vCaughtUp >>= check
    allOffsets <- readTVar vOffsets
    let finalOffset = case IM.maxViewWithKey allOffsets of
          Just ((k, _), _) -> k + 1
          Nothing -> 0
    let rotate i
          | i < 0 = finalOffset + i
          | otherwise = i
    begin <- case begin_ of
      BySeqNum i -> pure $ rotate i
      ByIndex index val -> case HM.lookup index indices of
        Nothing -> throwSTM $ IndexNotFound index $ HM.keys indices
        Just v -> do
          m <- readTVar v
          let (_, wing) = splitR val m
          return $! maybe maxBound fst $ IM.minView wing
    end <- case end_ of
      BySeqNum i -> pure $ rotate i
      ByIndex index val -> case HM.lookup index indices of
        Nothing -> throwSTM $ IndexNotFound index $ HM.keys indices
        Just v -> do
          m <- readTVar v
          let (body, lastItem, _) = IM.splitLookup val m
          let body' = maybe id (IM.insert val) lastItem body
          return $! maybe minBound fst $ IM.maxView body'
    return $! range begin end rt allOffsets
