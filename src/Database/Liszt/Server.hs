{-# LANGUAGE LambdaCase, OverloadedStrings #-}
module Database.Liszt.Server where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import Data.Binary as B
import Data.Binary.Get as B
import Data.Binary.Put as B
import Data.Int
import Data.Semigroup
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

handleConsumer :: FilePath
  -> TVar (M.IntMap Int64)
  -> WS.Connection -> IO ()
handleConsumer path vIndices conn = do
  -- start from the beginning of the stream
  vOffset <- newTVarIO Nothing
  forever $ do
    req <- WS.receiveData conn
    case decode req of
      Blocking -> do
        (pos, pos') <- atomically $ do
          m <- readTVar vIndices
          readTVar vOffset >>= \case
            Nothing -> do
              (ofs'@(_, pos), _) <- foldAlt $ M.minViewWithKey m
              writeTVar vOffset (Just ofs')
              return (0, pos)
            Just (ofs, pos) -> do
              ofs'@(_, pos') <- foldAlt $ M.lookupGT ofs m
              writeTVar vOffset (Just ofs')
              return (pos, pos')

        withBinaryFile path ReadMode $ \h -> do
          hSeek h AbsoluteSeek (fromIntegral pos)
          bs <- B.hGet h $ fromIntegral $ pos' - pos
          WS.sendBinaryData conn bs
      Seek ofs -> atomically $ do
        m <- readTVar vIndices
        ofsPos <- foldAlt $ M.lookupLE (fromIntegral ofs) m
        writeTVar vOffset (Just ofsPos)

handleProducer :: FilePath
  -> TVar (M.IntMap Int64)
  -> TVar Bool
  -> WS.Connection
  -> IO ()
handleProducer path vIndices vUpdate conn = forever $ do
  reqBS <- WS.receiveData conn
  case runGetOrFail get reqBS of
    Right (content, _, req) -> do
      maxOfs <- atomically
        $ fmap fst . M.maxViewWithKey <$> readTVar vIndices

      ofs <- case req of
        Write o
          | maybe True ((o >) . fromIntegral . fst) maxOfs -> return $ fromIntegral o
          | otherwise -> fail "Monotonicity violation"
        Sequential -> return $ maybe 0 (succ . fromIntegral . fst) maxOfs

      h <- openBinaryFile path AppendMode
      BL.hPut h content
      hClose h

      atomically $ do
        let pos' = maybe 0 snd maxOfs + BL.length content
        modifyTVar' vIndices $ M.insert ofs pos'
        writeTVar vUpdate True
    Left _ -> WS.sendClose conn ("Malformed request" :: B.ByteString)

loadIndices :: FilePath -> IO (M.IntMap Int64)
loadIndices path = doesFileExist path >>= \case
  False -> return M.empty
  True -> do
    n <- (`div`16) <$> fromIntegral <$> getFileSize path
    bs <- BL.readFile path
    return $! M.fromAscList $ runGet (replicateM n $ (,) <$> get <*> get) bs

saveIndices :: FilePath -> M.IntMap Int64 -> IO ()
saveIndices path m
  | M.null m = return ()
  | otherwise = BL.appendFile path
    $ B.runPut $ forM_ (M.toList m) $ \(k, v) -> B.put k >> B.put v

openLisztServer :: FilePath -> IO WS.ServerApp
openLisztServer path = do
  let ipath = path ++ ".indices"
  let ppath = path ++ ".payload"

  vIndices <- loadIndices ipath >>= newTVarIO

  -- Persist indices
  vUpdate <- newTVarIO False
  vLastSynced <- atomically $ do
    m <- readTVar vIndices
    newTVar $ fst . fst <$> M.maxViewWithKey m

  _ <- forkIO $ forever $ do
    w <- atomically $ do
      b <- readTVar vUpdate
      unless b retry

      ofs <- readTVar vLastSynced
      m <- readTVar vIndices
      let w = maybe m (\k -> let (_, _, r) = M.splitLookup k m in r) ofs
      forM_ (M.maxViewWithKey w)
        $ \((k, _), _) -> writeTVar vLastSynced (Just k)
      return w

    saveIndices ipath w

  return $ \pending -> case WS.requestPath (WS.pendingRequest pending) of
    "read" -> WS.acceptRequest pending >>= handleConsumer ppath vIndices
    "write" -> WS.acceptRequest pending >>= handleProducer ppath vIndices vUpdate
    p -> WS.rejectRequest pending ("Bad request: " <> p)
