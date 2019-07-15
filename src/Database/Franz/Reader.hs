{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
module Database.Franz.Reader where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Serialize
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import qualified Data.IntMap.Strict as IM
import Data.Maybe (isJust)
import GHC.Clock (getMonotonicTime)
import GHC.Generics (Generic)
import System.Directory
import System.FilePath
import System.IO
import System.FSNotify

data RequestType = AllItems | LastItem deriving (Show, Generic)
instance Serialize RequestType

data Query = Query
  { reqStream :: !B.ByteString
  , reqFromIndex :: !(Maybe B.ByteString)
  , reqToIndex :: !(Maybe B.ByteString)
  , reqType :: !RequestType
  , reqFrom :: !Int
  , reqTo :: !Int
  } deriving (Show, Generic)
instance Serialize Query

data Stream = Stream
  { vOffsets :: TVar (IM.IntMap Int)
  , indexNames :: [B.ByteString]
  , indices :: HM.HashMap B.ByteString (TVar (IM.IntMap Int))
  , vCount :: TVar Int
  , vCaughtUp :: TVar Bool
  , followThread :: ThreadId
  , payloadHandle :: Handle
  , vActivity :: TVar Activity
  }

type Activity = Either Double Int

addActivity :: Activity -> Activity
addActivity (Left _) = Right 0
addActivity (Right n) = Right (n + 1)

removeActivity :: IO (Activity -> Activity)
removeActivity = do
  now <- getMonotonicTime
  return $ \case
    Left _ -> Left now
    Right n
      | n <= 0 -> Left now
      | otherwise -> Right (n - 1)

closeStream :: Stream -> IO ()
closeStream = killThread . followThread

createStream :: WatchManager -> FilePath -> IO Stream
createStream man path = do
  let offsetPath = path </> "offsets"
  let payloadPath = path </> "payloads"
  exist <- doesFileExist offsetPath
  unless exist $ throwIO $ StreamNotFound offsetPath
  initialOffsetsBS <- B.readFile offsetPath
  payloadHandle <- openBinaryFile payloadPath ReadMode
  let getInts bs = either (throwIO . InternalError) pure
        $ runGet (replicateM (B.length bs `div` 8) (fromIntegral <$> getInt64le)) bs
  initialOffsets <- IM.fromList . zip [0..] <$> getInts initialOffsetsBS
  vOffsets <- newTVarIO initialOffsets
  vCaughtUp <- newTVarIO False
  vCount <- newTVarIO $ IM.size initialOffsets
  _ <- watchDir man path (\case
    Modified p _ _ | p == offsetPath -> True
    _ -> False)
    $ const $ atomically $ writeTVar vCaughtUp False

  indexNames <- B.lines <$> B.readFile (path </> "indices")
  vIndices <- forM indexNames $ \name -> do
    let indexPath = path </> "indices" ++ "." ++ B.unpack name
    initial <- B.readFile indexPath >>= getInts
    newTVarIO $ IM.fromList $ zip initial [0..]

  followThread <- forkIO $ withFile offsetPath ReadMode $ \h -> do
    forM_ (IM.maxViewWithKey initialOffsets) $ \((i, ofs), _) -> do
      hSeek h AbsoluteSeek $ fromIntegral $ succ i * 8
      hSeek payloadHandle AbsoluteSeek $ fromIntegral ofs
    forever $ do
      bs <- B.hGet h 8
      if B.null bs
        then do
          atomically $ writeTVar vCaughtUp True
          atomically $ readTVar vCaughtUp >>= check . not
        else do
          ofs <- either (throwIO . InternalError) (pure . fromIntegral) $ runGet getInt64le bs
          header <- B.hGet payloadHandle $ 8 * (length indexNames + 1)
          (len, indices) <- either (throwIO . InternalError) pure $ flip runGet header $ (,)
            <$> getInt64le
            <*> traverse (const getInt64le) indexNames
          hSeek payloadHandle RelativeSeek $ fromIntegral len
          atomically $ do
            i <- readTVar vCount
            modifyTVar' vOffsets $ IM.insert i ofs
            forM_ (zip vIndices indices) $ \(v, x) -> do
              modifyTVar' v $ IM.insert (fromIntegral x) i
            writeTVar vCount $! i + 1

  let indices = HM.fromList $ zip indexNames vIndices

  vActivity <- getMonotonicTime >>= newTVarIO . Left
  return Stream{..}

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

data FranzException = MalformedRequest !String
  | StreamNotFound !FilePath
  | IndexNotFound !B.ByteString ![B.ByteString]
  | InternalError !String
  | ClientError !String
  deriving (Show, Generic)
instance Serialize FranzException
instance Exception FranzException

data FranzReader = FranzReader
  { watchManager :: WatchManager
  , vStreams :: TVar (HM.HashMap B.ByteString Stream)
  , prefix :: FilePath
  }

reaper :: Double -- interval
  -> Double -- lifetime
  -> FranzReader -> IO ()
reaper int life FranzReader{..} = forever $ do
  now <- getMonotonicTime
  xs <- atomically $ do
    list <- newTVar []
    m <- readTVar vStreams
    m' <- forM m $ \s -> readTVar (vActivity s) >>= \case
      Left t | now - t >= life -> Nothing <$ modifyTVar list (s:)
      _ -> pure $ Just s
    writeTVar vStreams $! HM.mapMaybe id m'
    readTVar list
  mapM_ closeStream xs
  threadDelay $ floor $ int * 1e6

withFranzReader :: FilePath -> (FranzReader -> IO ()) -> IO ()
withFranzReader prefix k = do
  vStreams <- newTVarIO HM.empty
  withManager $ \watchManager -> k FranzReader{..}

handleQuery :: FranzReader
  -> FilePath
  -> Query
  -> IO (STM (Bool, Handle, [B.ByteString], QueryResult, IO ()))
handleQuery FranzReader{..} dir (Query name bindex_ eindex_ rt begin_ end_) = do
  streams <- atomically $ readTVar vStreams
  let path = prefix </> dir </> B.unpack name
  let streamId = B.pack dir <> name
  Stream{..} <- case HM.lookup streamId streams of
    Nothing -> do
      s <- createStream watchManager path
      atomically $ modifyTVar' vStreams $ HM.insert streamId s
      return s
    Just vStream -> return vStream
  return $ do
    modifyTVar' vActivity addActivity
    readTVar vCaughtUp >>= check
    allOffsets <- readTVar vOffsets
    let finalOffset = case IM.maxViewWithKey allOffsets of
          Just ((k, _), _) -> k + 1
          Nothing -> 0
    let rotate i
          | i < 0 = finalOffset + i
          | otherwise = i
    begin <- case bindex_ of
      Nothing -> pure $ rotate begin_
      Just index -> case HM.lookup index indices of
        Nothing -> throwSTM $ IndexNotFound index $ HM.keys indices
        Just v -> do
          m <- readTVar v
          let (_, wing) = splitR begin_ m
          return $! maybe maxBound fst $ IM.minView wing
    end <- case eindex_ of
      Nothing -> pure $ rotate end_
      Just index -> case HM.lookup index indices of
        Nothing -> throwSTM $ IndexNotFound index $ HM.keys indices
        Just v -> do
          m <- readTVar v
          let (body, lastItem, _) = IM.splitLookup end_ m
          let body' = maybe id (IM.insert end_) lastItem body
          return $! maybe minBound fst $ IM.maxView body'
    let (ready, result) = range begin end rt allOffsets
    let reset = removeActivity >>= atomically . modifyTVar' vActivity
    return (ready, payloadHandle, indexNames, result, reset)
