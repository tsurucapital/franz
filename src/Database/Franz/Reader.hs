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
  , vTimestamp :: TVar Double
  }

createStream :: WatchManager -> FilePath -> IO Stream
createStream man path = do
  let offsetPath = path </> "offsets"
  let payloadPath = path </> "payloads"
  exist <- doesFileExist offsetPath
  unless exist $ throwIO $ StreamNotFound offsetPath
  initialOffsetsBS <- B.readFile offsetPath
  payloadHandle <- openBinaryFile payloadPath ReadMode
  let getInts bs = either error id
        $ runGet (replicateM (B.length bs `div` 8) (fromIntegral <$> getInt64le)) bs
  let initialOffsets = IM.fromList $ zip [0..] $ getInts initialOffsetsBS
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
    initial <- getInts <$> B.readFile indexPath
    newTVarIO $ IM.fromList $ zip initial [0..]

  followThread <- forkIO $ withFile offsetPath ReadMode $ \h -> do
    forM_ (IM.maxViewWithKey initialOffsets) $ \((i, ofs), _) -> do
      hSeek h AbsoluteSeek $ fromIntegral $ i * 8
      hSeek payloadHandle AbsoluteSeek $ fromIntegral ofs
    forever $ do
      bs <- B.hGet h 8
      if B.null bs
        then do
          atomically $ writeTVar vCaughtUp True
          atomically $ readTVar vCaughtUp >>= check . not
        else do
          let ofs = either error fromIntegral $ runGet getInt64le bs
          header <- B.hGet payloadHandle $ 8 * (length indexNames + 1)
          (len, indices) <- either fail pure $ flip runGet header $ (,)
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

  vTimestamp <- getMonotonicTime >>= newTVarIO
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
  atomically $ do
    m <- readTVar vStreams
    m' <- forM m $ \s -> do
      t <- readTVar $ vTimestamp s
      return $ s <$ guard (now - t <= life)
    writeTVar vStreams $! HM.mapMaybe id m'
  threadDelay $ floor $ int * 1e6

withFranzReader :: FilePath -> (FranzReader -> IO ()) -> IO ()
withFranzReader prefix k = do
  vStreams <- newTVarIO HM.empty
  withManager $ \watchManager -> k FranzReader{..}

handleQuery :: FranzReader
  -> FilePath
  -> Query
  -> IO (STM (Bool, Handle, [B.ByteString], QueryResult))
handleQuery FranzReader{..} dir (Query name bindex_ eindex_ rt begin_ end_) = do
  streams <- atomically $ readTVar vStreams
  let path = prefix </> dir </> B.unpack name
  let streamId = B.pack dir <> name
  Stream{..} <- case HM.lookup streamId streams of
    Nothing -> do
      s <- createStream watchManager path
      atomically $ modifyTVar' vStreams $ HM.insert streamId s
      return s
    Just vStream -> do
      getMonotonicTime >>= atomically . writeTVar (vTimestamp vStream)
      return vStream
  return $ do
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

    return (ready, payloadHandle, indexNames, result)
