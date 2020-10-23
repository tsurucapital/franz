{-# LANGUAGE OverloadedStrings #-}
module Database.Franz.URI
  ( FranzPath
  , toFranzPath
  , fromFranzPath
  ) where

import qualified Data.ByteString as B
import Data.List (stripPrefix)
import Data.String
import Network.Socket (PortNumber)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Text.Read (readMaybe)

type FranzPath = (String, PortNumber, B.ByteString)

toFranzPath :: String -> Either String FranzPath
toFranzPath uri = do
  hostnamePath <- maybe (Left "Expecting franz://") Right $ stripPrefix "franz://" uri
  (host, path) <- case break (== '/') hostnamePath of
    (h, '/' : p) -> Right (h, p)
    _ -> Left "Expecting /"
  let path' = T.encodeUtf8 $ T.pack path
  case break (== ':') host of
    (hostname, ':' : portStr)
        | Just p <- readMaybe portStr -> Right (hostname, p, path')
        | otherwise -> Left "Failed to parse the port number"
    _ -> Right (host, 1886, path')

fromFranzPath :: (Monoid a, IsString a) => FranzPath -> a
fromFranzPath (host, port, path) = mconcat
  [ "franz://"
  , fromString host
  , ":"
  , fromString (show port)
  , "/"
  , fromString $ T.unpack $ T.decodeUtf8 path
  ]
