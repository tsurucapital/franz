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

-- | Parse a franz URI (franz://host:port/path).
toFranzPath :: String -> Either String FranzPath
toFranzPath uri = do
  hostnamePath <- maybe (Left "Expecting franz://") Right $ stripPrefix "franz://" uri
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
  [ "franz://"
  , fromString host
  , ":"
  , fromString (show port)
  , "/"
  , fromString path
  ]
