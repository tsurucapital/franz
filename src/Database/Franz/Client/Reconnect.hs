{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
module Database.Franz.Client.Reconnect
  ( Pool
  , poolLogFunc
  , poolRetryPolicy
  , withPool
  , withReconnection
  , Reconnect(..)
  , atomicallyReconnecting
  , fetchWithPool
  )
  where

import Control.Retry (recovering, RetryPolicyM)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Exception (IOException)
import Control.Monad.Catch
import Database.Franz.Client

data Pool = Pool
  { poolPath :: FranzPath
  , poolRef :: MVar (Int {- Connection number -}, Maybe Connection)
  , poolRetryPolicy :: RetryPolicyM IO
  , poolLogFunc :: String -> IO ()
  }

-- | A wrapper of 'fetch' which calls 'withReconnection' internally
fetchWithPool
  :: Pool
  -> Query
  -> (STM Response -> IO r)
  -> IO r
fetchWithPool pool q cont = withReconnection pool $ \conn -> fetch conn q cont
  `catch` \case
    r@(ReconnectInQuery _ _) -> throwM r -- Avoid deeply nested ReconnectInQuery
    r -> throwM $ ReconnectInQuery q r

-- | Run an action which takes a 'Connection', reconnecting whenever it throws an exception.
withReconnection :: Pool -> (Connection -> IO a) -> IO a
withReconnection Pool{..} cont = recovering
  poolRetryPolicy
  [const $ Handler $ \(_ :: Reconnect) -> pure True]
  body
  where

    handler ex
      | Just (ClientError err) <- fromException ex = Just err
      | Just e <- fromException ex = Just (show (e :: IOException))
      | Just (e :: Reconnect) <- fromException ex = Just
          $ "Reconnecting to " <> fromFranzPath poolPath <> " due to " <> show e
      | otherwise = Nothing

    body _ = do
      (i, conn) <- modifyMVar poolRef $ \case
        (i, Nothing) -> do
            poolLogFunc $ unwords
                [ "Connnecting to"
                , fromFranzPath poolPath
                ]
            conn <- tryJust handler (connect poolPath)
                >>= either (\e -> poolLogFunc e >> throwM ReconnectByError) pure
            poolLogFunc $ "Connection #" <> show i <> " established"
            pure ((i, Just conn), (i, conn))
        v@(i, Just c) -> pure (v, (i, c))

      tryJust handler (cont conn) >>= \case
        Right a -> pure a
        Left msg -> do
            poolLogFunc msg
            modifyMVar_ poolRef $ \case
                -- Don't disconnect if the sequential number is different;
                -- another thread already established a new connection
                (j, Just _) | i == j -> (i + 1, Nothing) <$ disconnect conn
                x -> pure x
            throwM ReconnectByError

data Reconnect = ReconnectByTimeout
  | ReconnectByError
  | ReconnectInQuery !Query !Reconnect
  deriving (Show, Eq)
instance Exception Reconnect

withPool :: RetryPolicyM IO
    -> (String -> IO ()) -- ^ diagnostic output
    -> FranzPath
    -> (Pool -> IO a)
    -> IO a
withPool poolRetryPolicy poolLogFunc poolPath cont = do
  poolRef <- newMVar (0, Nothing)
  cont Pool{..} `finally` do
    (_, conn) <- takeMVar poolRef
    mapM_ disconnect conn

-- | Run an 'STM' action, throwing 'Reconnect' when it exceeds the given timeout.
atomicallyReconnecting :: Int -- ^ timeout in microseconds
    -> STM a -> IO a
atomicallyReconnecting timeout m = atomicallyWithin timeout m
  >>= maybe (throwM ReconnectByTimeout) pure