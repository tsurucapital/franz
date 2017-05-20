{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where

import Control.Exception
import Control.Monad
import Database.Liszt.Client
import Data.Function (fix)
import qualified Data.ByteString.Char8 as B
import System.Environment
import System.IO
import System.IO.Error (isEOFError)

parseHostPort :: String -> (String -> Int -> r) -> r
parseHostPort str k = case break (==':') str of
  (host, ':' : port) -> k host (read port)
  (host, _) -> k host 6000

main :: IO ()
main = getArgs >>= \case
  ["subscribe", hostPort] -> parseHostPort hostPort withConsumer $ \conn -> forever $ do
    bs <- readBlocking conn
    B.hPut stdout bs
    putStrLn ""
  ["read", hostPort] -> parseHostPort hostPort withConsumer $ \conn -> fix
    $ \self -> readNonBlocking conn >>= \case
      Just bs -> do
        B.hPut stdout bs
        putStrLn ""
        self
      Nothing -> return ()
  ["write", hostPort] -> parseHostPort hostPort withProducer
    $ \conn -> handle (\e -> if isEOFError e then return () else throwIO e)
    $ forever $ do
    bs <- B.getLine
    writeSeqNo conn bs
  _ -> fail "Usage: liszt read host:port\nliszt write host:port"
