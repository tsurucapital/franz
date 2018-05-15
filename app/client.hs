{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where
import Database.Liszt

import Control.Exception
import Control.Monad
import Data.Function (fix)
import Data.Proxy
import qualified Data.ByteString.Char8 as B
import System.Environment
import System.IO
import System.IO.Error (isEOFError)

parseHostPort :: String -> (String -> Int -> r) -> r
parseHostPort str k = case break (==':') str of
  (host, ':' : port) -> k host (read port)
  (host, _) -> k host 1886

main :: IO ()
main = getArgs >>= \case
  ["read", hostPort, name, begin, end] -> parseHostPort hostPort withConnection $ \conn -> do
    bss <- fetch conn (Request (B.pack name) 1000000 (read begin) (read end))
    mapM_ (B.hPutStrLn stdout) bss
  ["write", path] -> withWriter path
    $ \h -> handle (\e -> if isEOFError e then return () else throwIO e)
    $ forever $ do
      bs <- B.getLine
      write h Proxy bs
  _ -> fail "Usage: liszt read host:port\nliszt write host:port"
