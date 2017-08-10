{-# LANGUAGE LambdaCase, ViewPatterns #-}

import Database.Liszt.Server
import Network.WebSockets as WS
import System.Console.GetOpt
import System.Environment
import qualified Data.ByteString.Char8 as BS
import qualified Data.HashMap.Strict as HM
import Control.Concurrent
import Control.Exception

hostPort :: [OptDescr ((String, Int) -> (String, Int))]
hostPort =
  [ Option ['p'] ["port"]
      (ReqArg (\x (h, _) -> (h, read x)) "PORT") "port"
  , Option ['h'] ["host"]
      (ReqArg (\x (_, p) -> (x, p)) "HOST") "host"
  ]

withHostPort :: [String] -> (String -> Int -> [String] -> IO ()) -> IO ()
withHostPort args k = case getOpt Permute hostPort args of
  (opts, xs, []) -> let (h, p) = foldl (flip id) ("127.0.0.1", 1886) opts
    in k h p $! xs
  (_, _, errs) -> ioError $ userError
    $ concat errs ++ usageInfo "Usage: lisztd [OPTION...] name" hostPort

main :: IO ()
main = getArgs >>= \args -> withHostPort args $ \host port _ -> do
  vServers <- newMVar HM.empty

  runServer host port $ \pending -> do
    let req = pendingRequest pending
    let (name, BS.drop 1 -> action) = BS.break (=='/') $ requestPath req

    m <- takeMVar vServers
    app <- case HM.lookup name m of
      Just app -> app <$ putMVar vServers m
      Nothing -> do
        (_, app) <- openLisztServer (BS.unpack name)
          `onException` putMVar vServers m
        putMVar vServers $! HM.insert name app m
        return app
    app pending { pendingRequest = req { requestPath = action } }
