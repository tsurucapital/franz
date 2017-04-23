{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where

import Control.Monad
import Database.Liszt.Client
import qualified Data.ByteString.Char8 as B
import System.Environment
import System.IO

parseHostPort :: String -> (String -> Int -> r) -> r
parseHostPort str k = case break (==':') str of
  (host, ':' : port) -> k host (read port)
  (host, _) -> k host 6000

main :: IO ()
main = getArgs >>= \case
  ["read", hostPort] -> parseHostPort hostPort withConsumer $ \conn -> forever $ do
    bs <- readBlocking conn
    B.hPut stdout bs
    putStrLn ""
  ["write", hostPort] -> parseHostPort hostPort withProducer $ \conn -> forever $ do
    bs <- B.getLine
    writeSeqNo conn bs
  _ -> fail "Usage: liszt read host:port\nliszt write host:port"
