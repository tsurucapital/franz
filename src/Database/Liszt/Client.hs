{-# LANGUAGE LambdaCase, OverloadedStrings #-}
module Database.Liszt.Client (
  -- * Consumer
  Consumer
  , withConsumer
  , readBlocking
  , readNonBlocking
  , seek
  , peek
  -- * Producer
  , Producer
  , withProducer
  , write
  , writeSeqNo
  ) where

import Control.Exception
import Control.Monad.IO.Class
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Int
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Database.Liszt.Types
import Network.WebSockets

-- | Connection as a consumer
newtype Consumer = Consumer Connection

-- | Acquire a consumer.
withConsumer :: String -> Int -> String -> (Consumer -> IO a) -> IO a
withConsumer host port name k = runClient host port ("/" ++ name ++ "/read") $ k . Consumer

parsePayload :: MonadIO m => BL.ByteString -> m (Int64, B.ByteString)
parsePayload bs = liftIO $ case runGetOrFail get bs of
  Right (content, _, ofs) -> return (ofs, BL.toStrict content)
  Left _ -> throwIO $ ParseException "Malformed response"

-- | Fetch a payload.
readBlocking :: MonadIO m => Consumer -> m (Int64, B.ByteString)
readBlocking (Consumer conn) = liftIO $ do
  sendBinaryData conn $ encode Read
  receiveData conn >>= parsePayload

-- | Fetch a payload. If it is at the end of the stream, return 'Nothing'.
readNonBlocking :: MonadIO m => Consumer -> m (Maybe (Int64, B.ByteString))
readNonBlocking (Consumer conn) = liftIO $ do
  sendBinaryData conn $ encode $ NonBlocking Read
  receiveDataMessage conn >>= \case
    Text "EOF" -> return Nothing
    Binary bs -> Just <$> parsePayload bs
    _ -> throwIO $ ParseException "Expecting EOF"

-- | Seek to a specicied position.
seek :: MonadIO m => Consumer -> Int64 -> m ()
seek (Consumer conn) ofs = liftIO $ sendBinaryData conn $ encode $ Seek ofs

-- | Returns the next offset.
peek :: MonadIO m => Consumer -> m Int64
peek (Consumer conn) = liftIO $ do
  sendBinaryData conn $ encode Peek
  decode <$> receiveData conn

-- | Connection as a producer
newtype Producer = Producer Connection

-- | Acquire a producer.
withProducer :: String -> Int -> String -> (Producer -> IO a) -> IO a
withProducer host port name k = runClient host port ("/" ++ name ++ "/write") $ k . Producer

-- | Write a payload with the specified offset. If the offset is less than the
-- last offset, it raises 'ConnectionException'.
write :: MonadIO m => Producer -> Int64 -> B.ByteString -> m ()
write (Producer conn) ofs bs = liftIO $ sendBinaryData conn $ runPut $ do
  put $ Write ofs
  putByteString bs

-- | Write a payload with an increasing natural number as an offset (starts from 0).
-- Atomic and non-blocking.
writeSeqNo :: MonadIO m => Producer -> B.ByteString -> m ()
writeSeqNo (Producer conn) bs = liftIO $ sendBinaryData conn $ runPut $ do
  put WriteSeqNo
  putByteString bs
