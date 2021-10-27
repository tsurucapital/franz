{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
module Database.Franz.Reader where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.State.Strict
import Control.Monad.Trans.Maybe
import Data.Hashable (Hashable)
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
import qualified System.FSNotify as FS

data StreamStatus = CaughtUp | Outdated | Gone deriving Eq

data Stream = Stream
  { streamPath :: FilePath
  , vOffsets :: !(TVar (IM.IntMap Int))
  , indexNames :: ![IndexName]
  , indices :: !(HM.HashMap IndexName (TVar (IM.IntMap Int)))
  , vCount :: !(TVar Int)
  , vStatus :: !(TVar StreamStatus)
  , followThread :: !ThreadId
  , indexHandle :: !Handle
  , payloadHandle :: !Handle
  , vActivity :: !(TVar Activity)
  }

type Activity = Either Double Int

addActivity :: Stream -> STM ()
addActivity str = modifyTVar' (vActivity str) $ \case
  Left _ -> Right 0
  Right n -> Right (n + 1)

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

createStream :: FS.WatchManager -> FilePath -> IO Stream
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
  vStatus <- newTVarIO Outdated
  vCount <- newTVarIO $! IM.size initialOffsets
  _ <- FS.watchDir man path (\case
    FS.Modified p _ _ | p == offsetPath -> True
    FS.Removed p _ _ | p == offsetPath -> True
    _ -> False)
    $ \case
      FS.Modified _ _ _ -> atomically $ writeTVar vStatus Outdated
      FS.Removed _ _ _ -> atomically $ writeTVar vStatus Gone
      _ -> pure ()

  vIndices <- forM [1..length indexNames] $ \i -> newTVarIO
    $ IM.fromList $ V.toList $ V.zip (V.map (U.! i) initialIndices) (V.enumFromN 0 count)

  indexHandle <- openFile offsetPath ReadMode

  let final :: Either SomeException Void -> IO ()
      final (Left exc) | Just ThreadKilled <- fromException exc = pure ()
      final (Left exc) = logFollower [path, "terminated with", show exc]
      final (Right v) = absurd v

  -- TODO broadcast an exception if it exits?
  followThread <- flip forkFinally final $ do
    forM_ (IM.maxViewWithKey initialOffsets) $ \((i, _), _) ->
      hSeek indexHandle AbsoluteSeek $ fromIntegral $ succ i * icount * 8
    forever $ do
      bs <- B.hGet indexHandle (8 * icount)
      if B.null bs
        then do
          atomically $ modifyTVar' vStatus $ \case
            Outdated -> CaughtUp
            CaughtUp -> CaughtUp
            Gone -> Gone
          atomically $ readTVar vStatus >>= check . (==Outdated)
        else do
          ofs : indices <- either (throwIO . InternalError) pure $ runGet (replicateM icount getI) bs
          atomically $ do
            i <- readTVar vCount
            modifyTVar' vOffsets $ IM.insert i ofs
            forM_ (zip vIndices indices) $ \(v, x) -> modifyTVar' v $ IM.insert (fromIntegral x) i
            writeTVar vCount $! i + 1

  let indices = HM.fromList $ zip indexNames vIndices

  vActivity <- getMonotonicTime >>= newTVarIO . Left
  return Stream{ streamPath = path, ..}
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
    AllItems -> (ready, (firstItem, maybe firstItem fst $ IM.maxViewWithKey body))
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
    firstItem = maybe zero fst $ IM.maxViewWithKey left

splitR :: Int -> IM.IntMap a -> (IM.IntMap a, IM.IntMap a)
splitR i m = let (l, p, r) = IM.splitLookup i m in (l, maybe id (IM.insert i) p r)

data FranzReader = FranzReader
  { watchManager :: FS.WatchManager
  , vStreams :: TVar (HM.HashMap FranzDirectory (HM.HashMap StreamName Stream))
  }

data ReaperState = ReaperState
    { -- | How many streams we pruned.
      prunedStreams :: !Int
      -- | How many streams we saw in total.
    , totalStreams :: !Int
    }

reaper :: Double -- interval
  -> Double -- lifetime
  -> FranzReader -> IO ()
reaper int life FranzReader{..} = forever $ do
  now <- getMonotonicTime
  -- Check if stream's activity indicates that we should prune it.
  let shouldPrune (Left t) = now - t >= life
      shouldPrune _ = False

      -- Try prunning stream at given filepath. Checks if stream
      -- should really be pruned first.
      tryPrune :: FranzDirectory -> StreamName -> STM (Maybe Stream)
      tryPrune mPath sPath = runMaybeT $ do
        currentAllStreams <- lift $ readTVar vStreams
        currentStreams <- MaybeT . pure $ HM.lookup mPath currentAllStreams
        currentStream <- MaybeT . pure $ HM.lookup sPath currentStreams
        currentAct <- lift $ readTVar (vActivity currentStream)
        guard $ shouldPrune currentAct
        let newStreams = HM.delete sPath currentStreams
        lift . writeTVar vStreams $ if HM.null newStreams
          -- Stream we're deleting was the
          -- last one around.
          then HM.delete mPath currentAllStreams
          -- Still have some other streams left for this mount path.
          -- Keep those only.
          else HM.insert mPath newStreams currentAllStreams
        pure currentStream

  -- Take a snapshots of all streams.
  allStreams <- readTVarIO vStreams
  -- Traverse the snapshot, looking for streams that currently seem
  -- out of date. If we find an out-of-date stream, take outer lock
  -- too, check again, delete it if necessary. Close stream promptly
  -- after deletion.
  --
  -- We could first traverse the whole snapshot, gather potential
  -- streams for deletion, take lock and delete all these streams from
  -- the map. However, on an assumption that we normally reaps streams
  -- and much lower rate than we use them and in favour of locking at
  -- as small time intervals as possible, we simply traverse the
  -- snapshot and if we find an out of date stream in the snapshot, we
  -- take the lock on the whole stream data structure and delete the
  -- stream. We also check that we aren't leaving an empty map entry
  -- behind and if we are, we delete it straight away. This stops us
  -- from having to traverse the whole map later to clean things up as
  -- well as ensuring that we never leave empty values in the map for
  -- others to see.
  --
  -- While doing all this, keep track of how many streams we saw and
  -- how many we have closed which is used later to report statistics
  -- to the user: this is in contrast to doing linear-time traversals
  -- just to get counts of things we've already traversed.
  stats <- flip execStateT (ReaperState 0 0) $
    forM_ (HM.toList allStreams) $ \(mPath, streams) ->
      forM_ (HM.toList streams) $ \(sPath, stream) -> do
        modify' $ \s -> s { totalStreams = totalStreams s + 1 }
        snapAct <- lift $ readTVarIO (vActivity stream)
        when (shouldPrune snapAct) $ do
          -- The stream indicates that it's old and should be pruned. Take
          -- a lock on the outer map, check again inside a transaction and
          -- amend the maps as necessary.
          deletedStream'm <- lift . atomically $ tryPrune mPath sPath
          forM_ deletedStream'm $ \prunedStream -> do
            lift $ closeStream prunedStream
            modify' $ \s -> s { prunedStreams = prunedStreams s + 1 }

  when (prunedStreams stats > 0) $ hPutStrLn stderr $ unwords
    [ "[reaper] closed"
    , show (prunedStreams stats)
    , "out of"
    , show (totalStreams stats)
    ]

  threadDelay $ floor $ int * 1e6

withFranzReader :: (FranzReader -> IO ()) -> IO ()
withFranzReader k = do
  vStreams <- newTVarIO HM.empty
  FS.withManager $ \watchManager -> k FranzReader{..}

-- | Globally-configured path which contains franz directories.
newtype FranzPrefix = FranzPrefix { unFranzPrefix :: FilePath } deriving (Eq, Hashable)

-- | Directory which contains franz streams.
-- Values of this type serve two purposes:
--
-- * Arbitrary prefix so that clients don't have to specify the full path 
newtype FranzDirectory = FranzDirectory FilePath deriving (Eq, Hashable)

getFranzDirectory :: FranzPrefix -> FranzDirectory -> FilePath
getFranzDirectory (FranzPrefix prefix) (FranzDirectory dir) = prefix </> dir

getFranzStreamPath :: FranzPrefix -> FranzDirectory -> StreamName -> FilePath
getFranzStreamPath prefix dir name
  = getFranzDirectory prefix dir </> streamNameToPath name

handleQuery :: FranzPrefix
  -> FranzReader
  -> FranzDirectory
  -> Query
  -> (Stream -> STM (Bool, QueryResult) -> IO r) -> IO r
handleQuery prefix FranzReader{..} dir (Query name begin_ end_ rt) cont
  = bracket acquire removeActivity
  $ \stream@Stream{..} -> cont stream $ do
    readTVar vStatus >>= \case
        Outdated -> retry
        CaughtUp -> pure ()
        Gone -> throwSTM $ StreamNotFound streamPath
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
  where
    acquire = join $ atomically $ do
      allStreams <- readTVar vStreams
      let !streams = maybe mempty id $ HM.lookup dir allStreams
      case HM.lookup name streams of
        Nothing -> pure $ bracketOnError
          (createStream watchManager $ getFranzStreamPath prefix dir name)
          closeStream $ \s -> atomically $ do
            addActivity s
            modifyTVar' vStreams $ HM.insert dir $ HM.insert name s streams
            pure s
        Just s -> do
          addActivity s
          pure (pure s)