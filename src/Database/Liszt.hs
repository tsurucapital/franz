{-# LANGUAGE DeriveGeneric, RecordWildCards, LambdaCase #-}
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
    defRequest,
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

openWriter :: Naming f => FilePath -> IO (WriterHandle f)
openWriter path = do
  createDirectoryIfMissing False path
  let payloadPath = path </> "payloads"
  let offsetPath = path </> "offsets"
  let indexPath = path </> "indices"
  alreadyExists <- doesFileExist payloadPath
  vOffset <- if alreadyExists
    then withFile payloadPath ReadMode hFileSize >>= newMVar . fromIntegral
    else newMVar 0
  hPayload <- openFile payloadPath AppendMode
  hOffset <- openFile offsetPath AppendMode
  liftIO $ hSetBuffering hOffset NoBuffering
  hIndices <- forM idents $ \s -> do
    h <- openFile (indexPath ++ "." ++ s) AppendMode
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
    B.hPutStr hOffset $! BL.toStrict $ encode ofs'
    sequence_ $ liftA2 (\h -> B.hPutStr h . BL.toStrict . encode) hIndices ixs
    hFlush hPayload
    ) `onException` putMVar vOffset ofs
  putMVar vOffset ofs'

data Request = Request
  { streamName :: !B.ByteString
  , reqTimeout :: !Int
  , reqFrom :: !Int
  , reqTo :: !Int
  } deriving Generic
instance Binary Request

defRequest :: B.ByteString -> Request
defRequest name = Request
  { streamName = name
  , reqTimeout = maxBound `div` 2
  , reqFrom = 0
  , reqTo = 0
  }

data Stream = Stream
  { vOffsets :: TVar (IM.IntMap Int)
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
  let initialOffsets = IM.fromList
        $ zip [0..]
        $ runGet (replicateM (B.length initialOffsetsBS `div` 8) get)
        $ BL.fromStrict initialOffsetsBS
  vOffsets <- newTVarIO initialOffsets
  vCaughtUp <- newTVarIO False
  vCount <- newTVarIO $ IM.size initialOffsets
  watch <- addWatch inotify [Modify] offsetPath $ \case
    Modified _ _ -> atomically $ writeTVar vCaughtUp False
    _ -> return ()
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
          atomically $ do
            i <- readTVar vCount
            modifyTVar vOffsets $ IM.insert i ofs
            writeTVar vCount $! i + 1)
    $ const $ removeWatch watch
  return Stream{..}

range :: Int -> Int -> IM.IntMap Int
  -> ( Bool -- has final element
    , [(Int, Int, Int)] -- (seqno, begin, end)
    )
range begin_ end_ allOffsets = (isJust lastItem || not (null cont)
  , [(i, ofs, ofs' - ofs) | (ofs, (i, ofs')) <- zip (firstOffset : map snd offsets) offsets])
  where
    finalOffset = case IM.maxViewWithKey allOffsets of
      Just ((k, _), _) -> k + 1
      Nothing -> 0
    begin
      | begin_ < 0 = finalOffset + begin_
      | otherwise = begin_
    end
      | end_ < 0 = finalOffset + end_
      | otherwise = end_

    (wing, lastItem, cont) = IM.splitLookup end allOffsets
    (left, firstItem, body) = IM.splitLookup begin
      $ maybe id (IM.insert end) lastItem wing
    offsets = IM.toList $ maybe id (IM.insert begin) firstItem body
    firstOffset = maybe 0 fst $ IM.maxView left

data LisztError = MalformedRequest
  | StreamNotFound deriving Show

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

handleRequest :: LisztReader -> Request -> IO (Handle, [(Int, Int, Int)])
handleRequest LisztReader{..} (Request name timeout begin end) = do
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

    let (ready, offsets) = range begin end allOffsets
    -- | If it timed out or has a matching element, continue
    timedout <- tryWaitDelay delay
    unless (timedout || ready) retry

    return (payloadHandle, offsets)

fetchLocal :: LisztReader -> Request -> IO [(Int, B.ByteString)]
fetchLocal env req = do
  (h, offsets) <- handleRequest env req
  forM offsets $ \(i, pos, len) -> do
    hSeek h AbsoluteSeek $ fromIntegral pos
    (,) i <$> B.hGet h len
