{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
module Database.Franz.Protocol
  ( apiVersion
  , defaultPort
  , IndexName
  , StreamName(..)
  , FranzException(..)
  , RequestType(..)
  , ItemRef(..)
  , Query(..)
  , RawRequest(..)
  , ResponseId
  , ResponseHeader(..)
  , PayloadHeader(..)) where

import Control.Exception (Exception)
import qualified Data.ByteString.Char8 as B
import Data.Serialize hiding (getInt64le)
import Database.Franz.Internal (getInt64le)
import Data.Hashable (Hashable)
import Data.String
import Network.Socket (PortNumber)
import GHC.Generics (Generic)
import qualified Data.ByteString.FastBuilder as BB

apiVersion :: B.ByteString
apiVersion = B.pack "0"

defaultPort :: PortNumber
defaultPort = 1886

type IndexName = B.ByteString

newtype StreamName = StreamName { unStreamName :: B.ByteString }
  deriving newtype (Show, Eq, Ord, Hashable, Serialize)

-- | UTF-8 encoded
instance IsString StreamName where
  fromString = StreamName . BB.toStrictByteString . BB.stringUtf8

data FranzException = MalformedRequest !String
  | StreamNotFound !FilePath
  | IndexNotFound !IndexName ![IndexName]
  | InternalError !String
  | ClientError !String
  deriving (Show, Generic)
instance Serialize FranzException
instance Exception FranzException

data RequestType = AllItems | LastItem deriving (Show, Generic)
instance Serialize RequestType

data ItemRef = BySeqNum !Int -- ^ sequential number
  | ByIndex !IndexName !Int -- ^ index name and value
  deriving (Show, Generic)
instance Serialize ItemRef

data Query = Query
  { reqStream :: !StreamName
  , reqFrom :: !ItemRef -- ^ name of the index to search
  , reqTo :: !ItemRef -- ^ name of the index to search
  , reqType :: !RequestType
  } deriving (Show, Generic)
instance Serialize Query

data RawRequest
  = RawRequest !ResponseId !Query
  | RawClean !ResponseId deriving Generic
instance Serialize RawRequest

type ResponseId = Int

data ResponseHeader = Response !ResponseId
    -- ^ response ID, number of streams; there are items satisfying the query
    | ResponseWait !ResponseId -- ^ response ID; requested elements are not available right now
    | ResponseError !ResponseId !FranzException -- ^ something went wrong
    deriving (Show, Generic)
instance Serialize ResponseHeader

-- | Initial seqno, final seqno, base offset, index names
data PayloadHeader = PayloadHeader !Int !Int !Int ![B.ByteString]

instance Serialize PayloadHeader where
  put (PayloadHeader s t u xs) = f s *> f t *> f u *> put xs where
    f = putInt64le . fromIntegral
  get = PayloadHeader <$> getInt64le <*> getInt64le <*> getInt64le <*> get
