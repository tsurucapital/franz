{-# LANGUAGE RecordWildCards, LambdaCase, OverloadedStrings, ViewPatterns, BangPatterns #-}
module Database.Liszt.Server (openLisztServer) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Binary as B
import Data.Binary.Get as B
import Data.Binary.Put as B
import Data.Int
import Data.Semigroup
import Data.String
import Database.Liszt.Types
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap.Strict as M
import qualified Network.WebSockets as WS
import System.Directory
import System.IO

data System = System
    { vPayload :: TVar (M.IntMap (B.ByteString, Int64))
    -- ^ A collection of payloads which are not available on disk.
    , vIndices :: TVar (M.IntMap Int64)
    -- ^ Map of byte offsets
    , vAccess :: TVar Bool
    -- ^ Is the payload file in use?
    , theHandle :: Handle
    -- ^ The handle of the payload
    }

-- | Set the 'TVar' True while running the action.
-- It blocks if the TVar is already True.
acquire :: TVar Bool -> IO a -> IO a
acquire v m = do
  atomically $ do
    b <- readTVar v
    if b then retry else writeTVar v True
  m `finally` atomically (writeTVar v False)

-- | Read the payload at the position in 'TVar', and update it by the next position.
fetchPayload :: System
  -> TVar (M.Key, Int64)
  -> STM (IO B.ByteString)
fetchPayload System{..} v = do
  (ofs, pos) <- readTVar v
  m <- readTVar vIndices
  case M.lookupGT ofs m of
    Just op@(_, pos') -> do
      writeTVar v op
      return $ acquire vAccess $ do
        hSeek theHandle AbsoluteSeek (fromIntegral pos)
        B.hGet theHandle $ fromIntegral $ pos' - pos
    Nothing -> M.lookupGT ofs <$> readTVar vPayload >>= \case
      Just (ofs', (bs, pos')) -> do
        writeTVar v (ofs', pos')
        return $ return bs
      Nothing -> retry

handleConsumer :: System -> WS.Connection -> IO ()
handleConsumer sys@System{..} conn = do
  -- start from the beginning of the stream
  vOffset <- newTVarIO (minBound, 0)

  let transaction (NonBlocking r) = transaction r
        <|> pure (WS.sendTextData conn ("EOF" :: BL.ByteString))
      transaction Read = (>>=WS.sendBinaryData conn) <$> fetchPayload sys vOffset
      transaction Peek = WS.sendBinaryData conn . encode . fst
          <$> readTVar vOffset
      transaction (Seek ofs) = do
        m <- readTVar vIndices
        mapM_ (writeTVar vOffset) $ M.lookupLE (fromIntegral ofs) m
        return $ return ()

  forever $ do
    req <- WS.receiveData conn
    case decodeOrFail req of
      Right (_, _, r) -> join $ atomically $ transaction r
      Left (_, _, e) -> WS.sendClose conn (fromString e :: B.ByteString)

handleProducer :: System -> WS.Connection -> IO ()
handleProducer System{..} conn = forever $ do
  reqBS <- WS.receiveData conn
  case runGetOrFail get reqBS of
    Right (BL.toStrict -> !content, _, req) -> join $ atomically $ do
      m <- readTVar vPayload

      maxOfs <- case M.maxViewWithKey m of
        Just ((k, (_, p)), _) -> return $ Just (k, p)
        Nothing -> fmap fst <$> M.maxViewWithKey <$> readTVar vIndices

      let push o p = let !p' = p + fromIntegral (B.length content)
            in return () <$ modifyTVar' vPayload (M.insert o (content, p'))

      case req of
        Write o -> case maxOfs of
          Just (k, p)
            | k <= fromIntegral o -> push (fromIntegral o) p
            | otherwise -> return
                $ WS.sendClose conn ("Monotonicity violation" :: B.ByteString)
          Nothing -> push (fromIntegral o) 0
        WriteSeqNo -> case maxOfs of
          Just (k, p) -> push (k + 1) p
          Nothing -> push 0 0

    Left _ -> WS.sendClose conn ("Malformed request" :: B.ByteString)

loadIndices :: FilePath -> IO (M.IntMap Int64)
loadIndices path = doesFileExist path >>= \case
  False -> return M.empty
  True -> do
    n <- (`div`16) <$> fromIntegral <$> getFileSize path
    bs <- BL.readFile path
    return $! M.fromAscList $ runGet (replicateM n $ (,) <$> get <*> get) bs

synchronise :: FilePath -> System -> IO ()
synchronise ipath System{..} = forever $ do
  m <- atomically $ do
    w <- readTVar vPayload
    when (M.null w) retry
    return w

  acquire vAccess $ do
    hSeek theHandle SeekFromEnd 0
    mapM_ (B.hPut theHandle . fst) m
    hFlush theHandle

  BL.appendFile ipath
    $ B.runPut $ forM_ (M.toList m) $ \(k, (_, p)) -> B.put k >> B.put p

  atomically $ do
    modifyTVar' vIndices $ M.union (fmap snd m)
    modifyTVar' vPayload $ flip M.difference m

-- | Start a liszt server. 'openLisztServer "foo"' creates two files:
--
-- * @foo.indices@: A list of offsets.
--
-- * @foo.payload@: All payloads concatenated as one file.
--
openLisztServer :: FilePath -> IO (ThreadId, WS.ServerApp)
openLisztServer path = do
  let ipath = path ++ ".indices"
  let ppath = path ++ ".payload"

  vIndices <- loadIndices ipath >>= newTVarIO

  vPayload <- newTVarIO M.empty

  vAccess <- newTVarIO False

  theHandle <- openBinaryFile ppath ReadWriteMode
  hSetBuffering theHandle (BlockBuffering Nothing)

  let sys = System{..}

  tid <- forkIO $ forever $ synchronise ipath sys
    `catch` \e -> hPutStrLn stderr $ "synchronise: " ++ show (e :: IOException)

  return (tid, \pending -> case WS.requestPath (WS.pendingRequest pending) of
    "read" -> WS.acceptRequest pending >>= handleConsumer sys
    "write" -> WS.acceptRequest pending >>= handleProducer sys
    p -> WS.rejectRequest pending ("Bad request: " <> p))
