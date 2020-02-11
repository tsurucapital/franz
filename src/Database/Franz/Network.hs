{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE BangPatterns #-}
module Database.Franz.Network
  ( defaultPort
  , Connection
  , withConnection
  , connect
  , disconnect
  , Query(..)
  , ItemRef(..)
  , RequestType(..)
  , defQuery
  , Response
  , awaitResponse
  , SomeIndexMap
  , Contents
  , fetch
  , fetchTraverse
  , fetchSimple
  , atomicallyWithin
  , FranzException(..)) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Trans.Cont (ContT(..))
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay
import Database.Franz.Protocol
import qualified Data.IntMap.Strict as IM
import Data.IORef
import Data.Int (Int64)
import Data.Serialize hiding (getInt64le)
import Database.Franz.Internal
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S

-- The protocol
--
-- Client                     Server
---  | ---- Archive prefix ---> |  Mounts P if possible
---  | <--- apiVersion -------- |
---  | ---- RawRequest i p ---> |
---  | ---- RawRequest j q ---> |
---  | ---- RawRequest k r ---> |
---  | <--- ResponseInstant i - |
---  | <--- result for p -----  |
---  | <--- ResponseWait j ---- |
---  | <--- ResponseWait k ---- |
---  | <--- ResponseDelayed j - |
---  | <--- result for q -----  |
--   | ----  RawClean i ---->   |
--   | ----  RawClean j ---->   |
--   | ----  RawClean k ---->   |

data Connection = Connection
  { connSocket :: MVar S.Socket
  , connReqId :: TVar Int
  , connStates :: TVar (IM.IntMap (ResponseStatus Contents))
  , connThread :: !ThreadId
  }

data ResponseStatus a = WaitingInstant
    | WaitingDelayed
    | Errored !FranzException
    | Available !a
    deriving (Show, Functor)

withConnection :: String -> S.PortNumber -> B.ByteString -> (Connection -> IO r) -> IO r
withConnection host port dir = bracket (connect host port dir) disconnect

connect :: String -> S.PortNumber -> B.ByteString -> IO Connection
connect host port dir = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) S.Stream (S.addrProtocol addr)
  S.setSocketOption sock S.NoDelay 1
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ encode dir
  readyMsg <- SB.recv sock 4096
  unless (readyMsg == apiVersion) $ case decode readyMsg of
    Right (ResponseError _ e) -> throwIO e
    e -> throwIO $ ClientError $ "Database.Franz.Network.connect: Unexpected response: " ++ show e

  connSocket <- newMVar sock
  connReqId <- newTVarIO 0
  connStates <- newTVarIO IM.empty
  buf <- newIORef B.empty
  connThread <- flip forkFinally (either throwIO pure) $ forever
    $ (>>=either (throwIO . ClientError) atomically) $ runGetRecv buf sock $ get >>= \case
      ResponseInstant i -> do
        resp <- getResponse
        return $ do
          m <- readTVar connStates
          case IM.lookup i m of
            Nothing -> pure ()
            Just WaitingInstant -> writeTVar connStates $! IM.insert i (Available resp) m
            e -> throwSTM $ ClientError $ "Unexpected state on ResponseInstant " ++ show i ++ ": " ++ show e
      ResponseWait i -> return $ do
        m <- readTVar connStates
        case IM.lookup i m of
          Nothing -> pure ()
          Just WaitingInstant -> writeTVar connStates $! IM.insert i WaitingDelayed m
          e -> throwSTM $ ClientError $ "Unexpected state on ResponseWait " ++ show i ++ ": " ++ show e
      ResponseDelayed i -> do
        resp <- getResponse
        return $ do
          m <- readTVar connStates
          case IM.lookup i m of
            Nothing -> pure ()
            Just WaitingDelayed -> writeTVar connStates $! IM.insert i (Available resp) m
            e -> throwSTM $ ClientError $ "Unexpected state on ResponseDelayed " ++ show i ++ ": " ++ show e
      ResponseError i e -> return $ do
        m <- readTVar connStates
        case IM.lookup i m of
          Nothing -> throwSTM e
          Just _ -> writeTVar connStates $! IM.insert i (Errored e) m
  return Connection{..}

disconnect :: Connection -> IO ()
disconnect Connection{..} = do
  killThread connThread
  withMVar connSocket S.close

defQuery :: B.ByteString -> Query
defQuery name = Query
  { reqStream = name
  , reqFrom = BySeqNum 0
  , reqTo = BySeqNum 0
  , reqType = AllItems
  }

type SomeIndexMap = HM.HashMap B.ByteString Int64

-- | (seqno, indices, payloads)
type Contents = V.Vector (Int, SomeIndexMap, B.ByteString)

-- | When it is 'Right', it might block until the content arrives.
type Response = Either Contents (STM Contents)

awaitResponse :: STM (Either a (STM a)) -> STM a
awaitResponse = (>>=either pure id)

getResponse :: Get Contents
getResponse = do
    PayloadHeader s0 s1 p0 names <- get
    let df = s1 - s0
    if df <= 0
        then pure mempty
        else do
            ixs <- V.replicateM df $ (,) <$> getInt64le <*> traverse (const getInt64le) names
            payload <- getByteString $ fst (V.unsafeLast ixs) - p0
            pure $ V.create $ do
                vres <- VGM.unsafeNew df
                let go i ofs0
                        | i >= df = pure ()
                        | otherwise = do
                              let (ofs1, indices) = V.unsafeIndex ixs i
                                  !m = HM.fromList $ zip names indices
                                  !bs = B.take (ofs1 - ofs0) $ B.drop (ofs0 - p0) payload
                                  !num = s0 + i + 1
                              VGM.unsafeWrite vres i (num, m, bs)
                              go (i + 1) ofs1
                go 0 p0
                return vres

-- | Fetch requested data from the server.
-- Termination of the continuation cancels the request, allowing flexible
-- control of its lifetime.
fetch :: Connection
  -> Query
  -> (STM Response -> IO r) -- ^ running the STM action blocks until the response arrives
  -> IO r
fetch Connection{..} req cont = do
  reqId <- atomically $ do
    i <- readTVar connReqId
    writeTVar connReqId $! i + 1
    modifyTVar' connStates $ IM.insert i WaitingInstant
    return i
  withMVar connSocket $ \sock -> SB.sendAll sock $ encode $ RawRequest reqId req
  let
    go = do
      m <- readTVar connStates
      case IM.lookup reqId m of
        Nothing -> return $ Left V.empty -- fetch ended; nothing to return
        Just WaitingInstant -> retry -- wait for an instant response
        Just (Available xs) -> do
          writeTVar connStates $! IM.delete reqId m
          return $ Left xs
        Just WaitingDelayed -> return $ Right $ do
          m' <- readTVar connStates
          case IM.lookup reqId m' of
            Nothing -> return V.empty -- fetch ended; nothing to return
            Just WaitingDelayed -> retry
            Just (Available xs) -> do
              writeTVar connStates $! IM.delete reqId m'
              return xs
            Just (Errored e) -> throwSTM e
            Just WaitingInstant -> throwSTM $ ClientError $ "fetch/WaitingDelayed: unexpected state WaitingInstant"
        Just (Errored e) -> throwSTM e
  cont go `finally` do
    join $ atomically $ do
      m <- readTVar connStates
      writeTVar connStates $! IM.delete reqId m
      -- If the response arrived, no need to send a clean request
      return $ when (IM.member reqId m)
        $ withMVar connSocket $ \sock -> SB.sendAll sock $ encode $ RawClean reqId


-- | Queries in traversable @t@ form an atomic request. The response will become
-- available once all the elements are available.
--
-- Generalisation to Traversable guarantees that the response preserves the
-- shape of the request.
fetchTraverse :: Traversable t => Connection -> t Query -> (STM (Either (t Contents) (STM (t Contents))) -> IO r) -> IO r
fetchTraverse conn reqs = runContT $ do
  tresps <- traverse (ContT . fetch conn) reqs
  return $ do
    resps <- sequence tresps
    case traverse (either Just (const Nothing)) resps of
      Just instant -> return $ Left instant
      Nothing -> return $ Right $ traverse (either pure id) resps

-- | Send a single query and wait for the result. If it timeouts, it returns an empty list.
fetchSimple :: Connection
  -> Int -- ^ timeout in microseconds
  -> Query
  -> IO Contents
fetchSimple conn timeout req = fetch conn req (fmap (maybe mempty id) . atomicallyWithin timeout . awaitResponse)

atomicallyWithin :: Int -- ^ timeout in microseconds
  -> STM a
  -> IO (Maybe a)
atomicallyWithin timeout m = do
  d <- newDelay timeout
  atomically $ fmap Just m `orElse` (Nothing <$ waitDelay d)
