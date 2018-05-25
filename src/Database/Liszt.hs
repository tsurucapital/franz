{-# LANGUAGE DeriveGeneric, RecordWildCards, LambdaCase, Rank2Types, ScopedTypeVariables #-}
module Database.Liszt (
    -- * Writer interface
    Naming(..),
    WriterHandle,
    openWriter,
    closeWriter,
    withWriter,
    write,
    -- * Reader
    Request(..),
    RequestType(..),
    defRequest,
    IndexMap,
    LisztError(..),
    LisztReader,
    withLisztReader,
    handleRequest,
    fetchLocal,
    ) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.Binary
import Data.Binary.Get
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import Data.Foldable (toList)
import Data.Functor.Identity
import qualified Data.HashMap.Strict as HM
import Data.Int
import qualified Data.IntMap.Strict as IM
import Data.Maybe (isJust)
import Data.Proxy
import GHC.Generics (Generic)
import System.Directory
import System.FilePath
import System.IO
import System.INotify

-- | Naming functor
class (Applicative f, Traversable f) => Naming f where
  -- | Names of the bases.
  idents :: f String

instance Naming Proxy where
  idents = Proxy

instance Naming Identity where
  idents = Identity ""

data WriterHandle f = WriterHandle
  { hPayload :: Handle
  , hOffset :: Handle
  , hIndices :: f Handle
  , vOffset :: MVar Int
  }

openWriter :: forall f. Naming f => FilePath -> IO (WriterHandle f)
openWriter path = do
  createDirectoryIfMissing False path
  let payloadPath = path </> "payloads"
  let offsetPath = path </> "offsets"
  let indexPath = path </> "indices"
  alreadyExists <- doesFileExist payloadPath
  vOffset <- if alreadyExists
    then withFile payloadPath ReadMode hFileSize >>= newMVar . fromIntegral
    else newMVar 0
  writeFile indexPath $ unlines $ toList (idents :: f String)
  hPayload <- openFile payloadPath AppendMode
  hOffset <- openFile offsetPath AppendMode
  liftIO $ hSetBuffering hOffset NoBuffering
  hIndices <- forM idents $ \s -> do
    h <- openFile (indexPath <.> s) AppendMode
    hSetBuffering h NoBuffering
    return h
  return WriterHandle{..}

closeWriter :: Foldable f => WriterHandle f -> IO ()
closeWriter WriterHandle{..} = do
  hClose hPayload
  hClose hOffset
  mapM_ hClose hIndices

withWriter :: Naming f => FilePath -> (WriterHandle f -> IO ()) -> IO ()
withWriter path = bracket (openWriter path) closeWriter

write :: Naming f => WriterHandle f -> f Int64 -> B.ByteString -> IO ()
write WriterHandle{..} ixs bs = mask $ \restore -> do
  ofs <- takeMVar vOffset
  let ofs' = ofs + B.length bs
  restore (do
    B.hPutStr hPayload bs
    sequence_ $ liftA2 (\h -> B.hPutStr h . BL.toStrict . encode) hIndices ixs
    B.hPutStr hOffset $! BL.toStrict $ encode ofs'
    hFlush hPayload
    ) `onException` putMVar vOffset ofs
  putMVar vOffset ofs'

data RequestType = AllItems | LastItem deriving Generic
instance Binary RequestType

data Request = Request
  { streamName :: !B.ByteString
  , reqFromIndex :: !(Maybe B.ByteString)
  , reqToIndex :: !(Maybe B.ByteString)
  , reqTimeout :: !Int
  , reqType :: !RequestType
  , reqFrom :: !Int
  , reqTo :: !Int
  } deriving Generic
instance Binary Request

defRequest :: B.ByteString -> Request
defRequest name = Request
  { streamName = name
  , reqFromIndex = Nothing
  , reqToIndex = Nothing
  , reqTimeout = maxBound `div` 2
  , reqFrom = 0
  , reqTo = 0
  , reqType = AllItems
  }

type IndexMap = HM.HashMap B.ByteString

data Stream = Stream
  { vOffsets :: TVar (IM.IntMap Int)
  , reverseIndices :: IndexMap (TVar (IM.IntMap Int))
  , indices :: IndexMap (TVar (IM.IntMap Int))
  , vCount :: TVar Int
  , vCaughtUp :: TVar Bool
  , followThread :: ThreadId
  , payloadHandle :: Handle
  }

createStream :: INotify -> FilePath -> IO Stream
createStream inotify path = do
  let offsetPath = path </> "offsets"
  let payloadPath = path </> "payloads"
  createDirectoryIfMissing False path
  initialOffsetsBS <- B.readFile offsetPath
  payloadHandle <- openBinaryFile payloadPath ReadMode
  let getInts = runGet (replicateM (B.length initialOffsetsBS `div` 8) get)
        . BL.fromStrict
  let initialOffsets = IM.fromList $ zip [0..] $ getInts initialOffsetsBS
  vOffsets <- newTVarIO initialOffsets
  vCaughtUp <- newTVarIO False
  vCount <- newTVarIO $ IM.size initialOffsets
  watch <- addWatch inotify [Modify] offsetPath $ \case
    Modified _ _ -> atomically $ writeTVar vCaughtUp False
    _ -> return ()

  indexNames <- B.lines <$> B.readFile (path </> "indices")
  (indices_, reverseIndices_, updateIndices) <- fmap unzip3 $ forM indexNames $ \name -> do
    h <- openBinaryFile (path </> "indices" <.> B.unpack name) ReadMode
    initial <- getInts <$> B.hGetContents h
    var <- newTVarIO $ IM.fromList $ zip initial [0..]
    revVar <- newTVarIO $ IM.fromList $ zip [0..] initial
    return ((name, var), (name, revVar), do
      bs <- B.hGet h 8
      let val = decode $ BL.fromStrict bs
      return $ \i -> do
        modifyTVar var $ IM.insert val i
        modifyTVar var $ IM.insert i val
      )
  let indices = HM.fromList indices_
  let reverseIndices = HM.fromList reverseIndices_

  followThread <- forkFinally (withFile offsetPath ReadMode $ \h -> do
    hSeek h SeekFromEnd 0
    forever $ do
      bs <- B.hGet h 8
      if B.null bs
        then do
          atomically $ writeTVar vCaughtUp True
          atomically $ readTVar vCaughtUp >>= \b -> when b retry
        else do
          let ofs = decode $ BL.fromStrict bs
          upd <- sequence updateIndices
          atomically $ do
            i <- readTVar vCount
            modifyTVar vOffsets $ IM.insert i ofs
            mapM_ ($ i) upd
            writeTVar vCount $! i + 1)
    $ const $ removeWatch watch

  return Stream{..}

range :: Int -- from
  -> Int -- to
  -> RequestType
  -> IM.IntMap Int -- offsets
  -> IndexMap (IM.IntMap Int) -- index snapshots
  -> ( Bool -- has final element
    , [(Int, IndexMap Int, Int, Int)] -- (seqno, begin, end)
    )
range begin end rt allOffsets snapshots = (isJust lastItem || not (null cont)
  , [(i, HM.mapMaybe (IM.lookup i) snapshots, ofs, ofs' - ofs)
    | (ofs, (i, ofs')) <- offsets])
  where
    (wing, lastItem, cont) = IM.splitLookup end allOffsets
    (left, body) = splitR begin $ maybe id (IM.insert end) lastItem wing
    offsets = case rt of
      AllItems -> let xs = IM.toList body
                      firstOffset = maybe 0 fst $ IM.maxView left
          in zip (firstOffset : map snd xs) xs
      LastItem -> case IM.maxViewWithKey body of
        Nothing -> []
        Just ((i, ofs'), r) -> case IM.maxView r of
          Just (ofs, _) -> [(ofs, (i, ofs'))]


splitR :: Int -> IM.IntMap a -> (IM.IntMap a, IM.IntMap a)
splitR i m = let (l, p, r) = IM.splitLookup i m in (l, maybe id (IM.insert i) p r)

data LisztError = MalformedRequest
  | StreamNotFound
  | IndexNotFound
  deriving Show
instance Exception LisztError

data LisztReader = LisztReader
  { inotify :: INotify
  , vStreams :: TVar (HM.HashMap B.ByteString Stream)
  , prefix :: FilePath
  }

withLisztReader :: FilePath -> (LisztReader -> IO ()) -> IO ()
withLisztReader prefix k = do
  vStreams <- newTVarIO HM.empty
  withINotify $ \inotify -> k LisztReader{..}

handleRequest :: LisztReader
  -> Request
  -> IO (Handle, [(Int, IndexMap Int, Int, Int)])
handleRequest LisztReader{..} (Request name bindex_ eindex_ timeout rt begin_ end_) = do
  streams <- atomically $ readTVar vStreams
  let path = prefix </> B.unpack name
  Stream{..} <- case HM.lookup name streams of
    Nothing -> do
      s <- createStream inotify path
      atomically $ modifyTVar vStreams $ HM.insert name s
      return s
    Just vStream -> return vStream
  delay <- newDelay timeout
  atomically $ do
    readTVar vCaughtUp >>= \b -> unless b retry
    allOffsets <- readTVar vOffsets
    indexSnapshots <- traverse readTVar reverseIndices
    let finalOffset = case IM.maxViewWithKey allOffsets of
          Just ((k, _), _) -> k + 1
          Nothing -> 0
    let rotate i
          | i < 0 = finalOffset + i
          | otherwise = i
    begin <- case bindex_ of
      Nothing -> pure $ rotate begin_
      Just index -> case HM.lookup index indices of
        Nothing -> throwSTM IndexNotFound
        Just v -> do
          m <- readTVar v
          let (_, wing) = splitR begin_ m
          return $! maybe maxBound fst $ IM.minView wing
    end <- case eindex_ of
      Nothing -> pure $ rotate end_
      Just index -> case HM.lookup index indices of
        Nothing -> throwSTM IndexNotFound
        Just v -> do
          m <- readTVar v
          let (body, lastItem, _) = IM.splitLookup end_ m
          let body' = maybe id (IM.insert end_) lastItem body
          return $! maybe minBound fst $ IM.maxView body'
    let (ready, offsets) = range begin end rt allOffsets indexSnapshots

    -- | If it timed out or has a matching element, continue
    timedout <- tryWaitDelay delay
    unless (timedout || ready) retry

    return (payloadHandle, offsets)

fetchLocal :: LisztReader -> Request -> IO [(Int, IndexMap Int, B.ByteString)]
fetchLocal env req = do
  (h, offsets) <- handleRequest env req
  forM offsets $ \(i, xs, pos, len) -> do
    hSeek h AbsoluteSeek $ fromIntegral pos
    (,,) i xs <$> B.hGet h len
