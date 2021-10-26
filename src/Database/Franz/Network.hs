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
  , atomicallyWithin
  , FranzException(..)) where

import Control.Arrow ((&&&))
import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.STM.Delay (newDelay, waitDelay)
import Control.Exception
import Control.Monad
import qualified Data.ByteString.Char8 as B
import Data.ConcurrentResourceMap
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
import Database.Franz.URI
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
    -- | The user cancelled the request.
    | RequestFinished
    deriving (Show, Functor)

withConnection :: FranzPath -> (Connection -> IO r) -> IO r
withConnection path = bracket (connect path) disconnect

connect :: FranzPath -> IO Connection
connect (FranzPath host port dir) = do
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
        Just reqVar -> atomically $ readTVar reqVar >>= \case
          -- If request is finished, do nothing to the content.
          RequestFinished -> void $ f Nothing
          s -> f (Just s) >>= mapM_ (writeTVar reqVar)

      runGetThrow :: Get a -> IO a
      runGetThrow g = runGetRecv buf sock g
        >>= either (throwIO . ClientError) pure

  connThread <- flip forkFinally (either throwIO pure) $ forever $ runGetThrow get >>= \case
    Response i -> do
      resp <- runGetThrow getResponse
      withRequest i . traverse $ \case
        WaitingInstant -> pure (Available resp)
        WaitingDelayed -> pure (Available resp)
        e -> throwSTM $ ClientError $ "Unexpected state on ResponseInstant " ++ show i ++ ": " ++ show e
    ResponseWait i -> withRequest i . traverse $ \case
      WaitingInstant -> pure WaitingDelayed
      e -> throwSTM $ ClientError $ "Unexpected state on ResponseWait " ++ show i ++ ": " ++ show e
    ResponseError i e -> withRequest i $ \case
      Nothing -> throwSTM e
      Just{} -> pure $ Just (Errored e)
  return Connection{..}

disconnect :: Connection -> IO ()
disconnect Connection{..} = do
  killThread connThread
  withMVar connSocket S.close

defQuery :: StreamName -> Query
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
fetch
  :: Connection
  -> Query
  -> (STM Response -> IO r)
  -- ^ Wait for the response in a blocking manner. You should only run
  -- the continuation inside a 'fetch' block: leaking the STM action
  -- and running it outside will result in a 'ClientError' exception.
  -> IO r
fetch Connection{..} req cont = do
  reqId <- atomicAddCounter connReqId 1

  let -- When we exit the scope of the request, ensure that we cancel any
      -- outstanding request and set the appropriate state, lest the user
      -- leaks the resource and tries to re-run the provided action.
      cleanupRequest reqVar = do
        let inFlight WaitingInstant = True
            inFlight WaitingDelayed = True
            inFlight _ = False
        -- Check set the internal state to RequestFinished while
        -- noting if there's possibly a request still in flight.
        requestInFlight <- atomically $
          stateTVar reqVar $ inFlight &&& const RequestFinished
        when requestInFlight $ withMVar connSocket $ \sock ->
          SB.sendAll sock $ encode $ RawClean reqId

  -- We use a shared resource map here to ensure that we only hold
  -- onto the share connection state TVar for the duration of making a
  -- fetch request. If anything goes wrong in the middle, we're
  -- certain it'll get removed.
  withSharedResource connStates reqId
    (newTVarIO WaitingInstant)
    cleanupRequest $ \reqVar -> do
    -- Send the user request.
    withMVar connSocket $ \sock -> SB.sendAll sock $ encode
      $ RawRequest reqId req

    let
      requestFinished = ClientError "request already finished"

      getDelayed = readTVar reqVar >>= \case
        RequestFinished -> throwSTM requestFinished
        WaitingDelayed -> retry
        Available xs -> return xs
        Errored e -> throwSTM e
        WaitingInstant -> throwSTM $ ClientError
          "fetch/WaitingDelayed: unexpected state WaitingInstant"

    -- Run the user's continuation. 'withSharedResource' takes care of
    -- any clean-up necessary.
    cont $ readTVar reqVar >>= \case
      RequestFinished -> throwSTM requestFinished
      Errored e -> throwSTM e
      WaitingInstant -> retry -- wait for an instant response
      Available xs -> pure $ Left xs
      WaitingDelayed -> pure $ Right getDelayed

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
