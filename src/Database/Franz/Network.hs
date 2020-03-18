{-# LANGUAGE LambdaCase, OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TypeFamilies #-}
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
  , fetchSimple
  , FranzException(..)) where

import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import qualified Data.ByteString.Char8 as B
import Data.ConcurrentResourceMap
import Data.Either (isRight)
import qualified Data.HashMap.Strict as HM
import Data.IORef
import Data.IORef.Unboxed
import Data.Int (Int64)
import qualified Data.IntMap.Strict as IM
import Data.Serialize hiding (getInt64le)
import qualified Data.Vector as V
import qualified Data.Vector.Generic.Mutable as VGM
import Database.Franz.Internal
import Database.Franz.Protocol
import qualified Network.Socket as S
import qualified Network.Socket.ByteString as SB

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

newtype ConnStateMap v = ConnStateMap (IM.IntMap v)

instance ResourceMap ConnStateMap where
  type Key ConnStateMap = Int
  empty = ConnStateMap IM.empty
  delete k (ConnStateMap m) = ConnStateMap (IM.delete k m)
  insert k v (ConnStateMap m) = ConnStateMap (IM.insert k v m)
  lookup k (ConnStateMap m) = IM.lookup k m

data Connection = Connection
  { connSocket :: MVar S.Socket
  , connReqId :: !Counter
  , connStates :: !(ConcurrentResourceMap
                     ConnStateMap
                     (TVar (ResponseStatus Contents)))
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
  connReqId <- newCounter 0
  connStates <- newResourceMap
  buf <- newIORef B.empty
  let -- Get a reference to shared state for the request if it exists.
      withRequest i f = withInitialisedResource connStates i (\_ -> pure ()) $ \case
        Nothing ->
          -- If it throws an exception on no value, great, it will
          -- float out here. If it returns a value, it'll just be
          -- ignore as we can't do anything with it anyway.
          void $ atomically (f Nothing)
        Just reqVar -> atomically $
          readTVar reqVar >>= f . Just >>= mapM_ (writeTVar reqVar)

      runGetThrow :: Get a -> IO a
      runGetThrow g = runGetRecv buf sock g
        >>= either (throwIO . ClientError) pure

  connThread <- flip forkFinally (either throwIO pure) $ forever $ runGetThrow get >>= \case
    ResponseInstant i -> do
      resp <- runGetThrow getResponse
      withRequest i . traverse $ \case
        WaitingInstant -> pure (Available resp)
        e -> throwSTM $ ClientError $ "Unexpected state on ResponseInstant " ++ show i ++ ": " ++ show e
    ResponseWait i -> withRequest i . traverse $ \case
      WaitingInstant -> pure WaitingDelayed
      e -> throwSTM $ ClientError $ "Unexpected state on ResponseWait " ++ show i ++ ": " ++ show e
    ResponseDelayed i -> do
      resp <- runGetThrow getResponse
      withRequest i . traverse $ \case
        WaitingDelayed -> pure (Available resp)
        e -> throwSTM $ ClientError $ "Unexpected state on ResponseDelayed " ++ show i ++ ": " ++ show e
    ResponseError i e -> withRequest i $ \case
      Nothing -> throwSTM e
      Just{} -> pure $ Just (Errored e)
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

type SomeIndexMap = HM.HashMap IndexName Int64

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
--
-- Termination of 'fetch' continuation cancels the request, allowing
-- flexible control of its lifetime.
fetch :: Connection
  -> Query
  -> (Contents -> IO r)
  -- ^ Action to run on results that are available instantly.
  -> Either (IO r) (IO (Contents -> IO r))
  -- ^ Action to run if results are delayed. If we don't want to wait
  -- for delayed results, use 'Left' as a default action. 'Right'
  -- action is ran on delayed result when it becomes available.
  -> IO r
fetch Connection{..} req onInstant onDelayed'e = do
  reqId <- atomicAddCounter connReqId 1
  -- We use a shared resource map here to ensure that we only hold
  -- onto the share connection state TVar for the duration of making a
  -- fetch request. If anything goes wrong in the middle, we're
  -- certain it'll get removed.
  withSharedResource connStates reqId (newTVarIO WaitingInstant) (\_ -> pure ()) $ \reqVar -> do

    let allowDelayedResponse = isRight onDelayed'e
    -- Send the user request.
    withMVar connSocket $ \sock -> SB.sendAll sock $ encode
      $ RawRequest reqId req allowDelayedResponse
    let -- Cancel the request with the server if we catch an
        -- exception, such as user giving up on waiting and killing
        -- the action.
      cancelRequest = withMVar connSocket $ \sock ->
        SB.sendAll sock $ encode $ RawClean reqId

      getInstant = readTVar reqVar >>= \case
        Errored e -> throwSTM e
        WaitingInstant -> retry -- wait for an instant response
        Available xs -> return $ Just xs
        WaitingDelayed -> pure Nothing

      getDelayed = readTVar reqVar >>= \case
        WaitingDelayed -> retry
        Available xs -> return xs
        Errored e -> throwSTM e
        WaitingInstant -> throwSTM $ ClientError $
          "fetch/WaitingDelayed: unexpected state WaitingInstant"

    instant'm <- atomically getInstant `onException` cancelRequest
    case instant'm of
      -- We got an answer instantly, run user-callback.
      Just xs -> onInstant xs
      -- We were asked to wait.
      Nothing -> case onDelayed'e of
        -- User is dis-interested in waiting for results. We have
        -- indicated this in the request itself, therefore we don't
        -- have to cancel anything and we can just run the default
        -- user action.
        Left dontWait -> dontWait
        -- User is interested in waiting, at least for now. Initialise
        -- any context the user may need and then wait for results.
        Right onDelayedOuter -> do
          -- Get result handler. If this action fails, we want to
          -- cancel the request as we obviously won't be able to
          -- handle any results anyway.
          onDelayedResult <- onDelayedOuter `onException` cancelRequest
          -- Actually wait for the result. If we fail (such as by user
          -- killing the fetch action via a timeout), cancel the
          -- request as we're no longer able to handle it even if it
          -- comes in.
          delayedResult <- atomically getDelayed `onException` cancelRequest
          -- We got the result, we can finally handle it.
          onDelayedResult delayedResult

-- | Send a single query and wait for the result. If it timeouts, it returns an empty list.
fetchSimple :: Connection
  -> Int -- ^ timeout in microseconds
  -> Query
  -> IO Contents
fetchSimple conn t req = do
  startedBlocking <- newEmptyMVar
  let blockingFetch = fetch conn req pure $ Right $ do
        -- Tell the outer world that we're going to block waiting for
        -- an answer.
        putMVar startedBlocking ()
        pure pure
      -- After fetch started blocking (waiting for delayed response),
      -- race it with a thread that times out after @t@.
      blockingTimeout = do
        takeMVar startedBlocking
        threadDelay t
        pure mempty
  -- 'race' ensures to re-throw exceptions thrown by first action to
  -- terminate. This means that even if 'fetch' terminates early, we
  -- won't deadlock waiting for 'startedBlocking' as the other action
  -- will be 'cancel'led.
  either id id <$> race blockingFetch blockingTimeout
