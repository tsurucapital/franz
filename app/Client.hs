{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where

import Control.Monad
import Data.Binary as B
import Data.Binary.Put as B
import Database.Liszt.Types
import Network.WebSockets as WS
import Options
import qualified Data.ByteString.Char8 as B
import System.Environment
import System.IO

main :: IO ()
main = getArgs >>= \args -> withHostPort args
  $ \host port args' -> case args' of
    ["read"] -> runClient host port "read" $ \conn -> forever $ do
      sendBinaryData conn $ B.encode Blocking
      bs <- receiveData conn
      B.hPut stdout bs
      putStrLn ""
    ["write"] -> runClient host port "write" $ \conn -> forever $ do
      bs <- B.getLine
      sendBinaryData conn $ B.runPut $ do
        put Sequential
        putByteString bs
    _ -> fail "Usage: liszt read or liszt write"
