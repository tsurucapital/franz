{-# LANGUAGE RecordWildCards, LambdaCase, OverloadedStrings, ViewPatterns, BangPatterns #-}
module Database.Liszt.Server where

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

foldAlt :: Alternative f => Maybe a -> f a
foldAlt (Just a) = pure a
foldAlt Nothing = empty

data System = System
    { vPayload :: TVar (M.IntMap (B.ByteString, Int64))
    -- A collection of payloads which are not available on disk.
    , vIndices :: TVar (M.IntMap Int64)
    , vAccess :: TVar Bool
    , theHandle :: Handle
    }

acquire :: TVar Bool -> IO a -> IO a
acquire v m = do
  atomically $ do
    b <- readTVar v
    if b then retry else writeTVar v True
  m `finally` atomically (writeTVar v False)

handleConsumer :: System
  -> WS.Connection -> IO ()
handleConsumer System{..} conn = do
  -- start from the beginning of the stream
  vOffset <- newTVarIO Nothing

  let getPos = readTVar vOffset >>= \case
        Nothing -> do
          m <- readTVar vIndices
          (ofs'@(_, pos), _) <- foldAlt $ M.minViewWithKey m
          writeTVar vOffset (Just ofs')
          return $ Left (0, pos)
        Just (ofs, pos) -> do
          m <- readTVar vIndices
          case M.lookupGT ofs m of
            Just op@(_, pos') -> do
              writeTVar vOffset (Just op)
              return $ Left (pos, pos')
            Nothing -> M.lookupGT ofs <$> readTVar vPayload >>= \case
              Just (ofs', (bs, pos')) -> do
                writeTVar vOffset $ Just (ofs', pos')
                return $ Right bs
              Nothing -> retry

  let fetch (Left (pos, pos')) = acquire vAccess $ do
        hSeek theHandle AbsoluteSeek (fromIntegral pos)
        B.hGet theHandle $ fromIntegral $ pos' - pos
      fetch (Right bs) = return bs

  let sendEOF = WS.sendTextData conn ("EOF" :: BL.ByteString)

  let transaction (NonBlocking r) = transaction r <|> pure sendEOF
      transaction Read = (fetch >=> WS.sendBinaryData conn) <$> getPos
      transaction (Seek ofs) = do
        m <- readTVar vIndices
        ofsPos <- foldAlt $ M.lookupLE (fromIntegral ofs) m
        writeTVar vOffset (Just ofsPos)
        return $ WS.sendTextData conn ("Done" :: BL.ByteString)

  forever $ do
    req <- WS.receiveData conn
    case decodeOrFail req of
      Right (_, _, r) -> join $ atomically $ transaction r
      Left (_, _, e) -> WS.sendClose conn (fromString e :: B.ByteString)

handleProducer :: System
  -> WS.Connection
  -> IO ()
handleProducer System{..} conn = forever $ do
  reqBS <- WS.receiveData conn
  case runGetOrFail get reqBS of
    Right (BL.toStrict -> !content, _, req) -> join $ atomically $ do

      let !len = fromIntegral (B.length content)

      let g o p = return ()
            <$ modifyTVar' vPayload (M.insert o (content, p + len))

      m <- readTVar vPayload

      maxOfs <- case M.maxViewWithKey m of
        Just ((k, (_, p)), _) -> return $ Just (k, p)
        Nothing -> fmap fst <$> M.maxViewWithKey <$> readTVar vIndices

      case req of
        Write o -> case maxOfs of
          Just (k, p)
            | k <= fromIntegral o -> g (fromIntegral o) p
            | otherwise -> return
                $ WS.sendClose conn ("Monotonicity violation" :: B.ByteString)
          Nothing -> g (fromIntegral o) 0
        WriteSeqNo -> case maxOfs of
          Just (k, p) -> g (k + 1) p
          Nothing -> g 0 0

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

  BL.appendFile ipath
    $ B.runPut $ forM_ (M.toList m) $ \(k, (_, p)) -> B.put k >> B.put p

  atomically $ do
    modifyTVar' vIndices $ M.union (fmap snd m)
    modifyTVar' vPayload $ flip M.difference m

openLisztServer :: FilePath -> IO WS.ServerApp
openLisztServer path = do
  let ipath = path ++ ".indices"
  let ppath = path ++ ".payload"

  vIndices <- loadIndices ipath >>= newTVarIO

  vPayload <- newTVarIO M.empty

  vAccess <- newTVarIO False

  theHandle <- openBinaryFile ppath ReadWriteMode

  let sys = System{..}

  _ <- forkIO $ forever $ synchronise ipath sys
    `catch` \e -> hPutStrLn stderr $ "synchronise: " ++ show (e :: IOException)

  return $ \pending -> case WS.requestPath (WS.pendingRequest pending) of
    "read" -> WS.acceptRequest pending >>= handleConsumer sys
    "write" -> WS.acceptRequest pending >>= handleProducer sys
    p -> WS.rejectRequest pending ("Bad request: " <> p)
