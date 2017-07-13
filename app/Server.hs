{-# LANGUAGE LambdaCase #-}

import Database.Liszt.Server
import Network.WebSockets as WS
import System.Console.GetOpt
import System.Environment

hostPort :: [OptDescr ((String, Int) -> (String, Int))]
hostPort =
  [ Option ['p'] ["port"]
      (ReqArg (\x (h, _) -> (h, read x)) "PORT") "port"
  , Option ['h'] ["host"]
      (ReqArg (\x (_, p) -> (x, p)) "HOST") "host"
  ]

withHostPort :: [String] -> (String -> Int -> [String] -> IO ()) -> IO ()
withHostPort args k = case getOpt Permute hostPort args of
  (opts, xs, []) -> let (h, p) = foldl (flip id) ("127.0.0.1", 6000) opts
    in k h p $! xs
  (_, _, errs) -> ioError $ userError
    $ concat errs ++ usageInfo "Usage: lisztd [OPTION...] name" hostPort

main :: IO ()
main = getArgs >>= \args -> withHostPort args $ \host port [path] -> do
  (_, app) <- openLisztServer path

  runServer host port app
