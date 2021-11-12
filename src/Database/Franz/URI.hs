{-# LANGUAGE OverloadedStrings #-}
module Database.Franz.URI
  ( FranzPath(..)
  , toFranzPath
  , fromFranzPath
  ) where

import Data.List (stripPrefix)
import Data.String
import Network.Socket (HostName, PortNumber)
import Text.Read (readMaybe)

data FranzPath = FranzPath
  { franzHost :: !HostName
  , franzPort :: !PortNumber
  , franzDir :: !FilePath
  -- ^ Prefix of franz directories
  }
  | LocalFranzPath !FilePath
  deriving (Show, Eq, Ord)

localPrefix :: IsString a => a
localPrefix = "franz-local:"

remotePrefix :: IsString a => a
remotePrefix = "franz://"

-- | Parse a franz URI (franz://host:port/path or franz-local:path).
toFranzPath :: String -> Either String FranzPath
toFranzPath uri | Just path <- stripPrefix localPrefix uri = Right $ LocalFranzPath path
toFranzPath uri = do
  hostnamePath <- maybe (Left $ "Expecting " <> remotePrefix) Right $ stripPrefix remotePrefix uri
  (host, path) <- case break (== '/') hostnamePath of
    (h, '/' : p) -> Right (h, p)
    _ -> Left "Expecting /"
  case break (== ':') host of
    (hostname, ':' : portStr)
        | Just p <- readMaybe portStr -> Right $ FranzPath hostname p path
        | otherwise -> Left "Failed to parse the port number"
    _ -> Right $ FranzPath host 1886 path

-- | Render 'FranzPath' as a franz URI.
fromFranzPath :: (Monoid a, IsString a) => FranzPath -> a
fromFranzPath (FranzPath host port path) = mconcat
  [ remotePrefix
  , fromString host
  , ":"
  , fromString (show port)
  , "/"
  , fromString path
  ]

fromFranzPath (LocalFranzPath path) = localPrefix <> fromString path
