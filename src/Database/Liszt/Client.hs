{-# LANGUAGE LambdaCase, OverloadedStrings #-}
module Database.Liszt.Client (
  -- * Consumer
  Consumer
  , withConsumer
  , readBlocking
  , readNonBlocking
  , seek
  -- * Producer
  , Producer
  , withProducer
  , write
  , writeSeqNo
  ) where

import Control.Exception
import Control.Monad.IO.Class
import Data.Binary
import Data.Binary.Put
import Data.Int
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Database.Liszt.Types
import Network.WebSockets

newtype Consumer = Consumer Connection

withConsumer :: String -> Int -> (Consumer -> IO a) -> IO a
withConsumer host port k = runClient host port "read" $ k . Consumer

readBlocking :: MonadIO m => Consumer -> m B.ByteString
readBlocking (Consumer conn) = liftIO $ do
  sendBinaryData conn $ encode Read
  receiveData conn

readNonBlocking :: MonadIO m => Consumer -> m (Maybe B.ByteString)
readNonBlocking (Consumer conn) = liftIO $ do
  sendBinaryData conn $ encode $ NonBlocking Read
  receiveDataMessage conn >>= \case
    Text "EOF" -> return Nothing
    Binary bs -> return $ Just $! BL.toStrict bs
    _ -> throwIO $ ParseException "Expecting EOF"

seek :: MonadIO m => Consumer -> Int64 -> m ()
seek (Consumer conn) ofs = liftIO $ do
  sendBinaryData conn $ encode $ Seek ofs

newtype Producer = Producer Connection

withProducer :: String -> Int -> (Producer -> IO a) -> IO a
withProducer host port k = runClient host port "write" $ k . Producer

write :: MonadIO m => Producer -> Int64 -> B.ByteString -> m ()
write (Producer conn) ofs bs = liftIO $ sendBinaryData conn $ runPut $ do
  put $ Write ofs
  putByteString bs

writeSeqNo :: MonadIO m => Producer -> B.ByteString -> m ()
writeSeqNo (Producer conn) bs = liftIO $ sendBinaryData conn $ runPut $ do
  put WriteSeqNo
  putByteString bs
