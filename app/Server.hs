{-# LANGUAGE LambdaCase #-}

import Database.Liszt.Server
import Network.WebSockets as WS
import Options
import System.Environment

main :: IO ()
main = getArgs >>= \args -> withHostPort args $ \host port [path] -> do
  app <- openLisztServer path

  runServer host port app
