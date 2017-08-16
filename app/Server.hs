{-# LANGUAGE LambdaCase, ViewPatterns #-}

import Database.Liszt.Server
import Network.WebSockets as WS
import System.Console.GetOpt
import System.Environment
import System.FilePath
import qualified Data.ByteString.Char8 as BS
import qualified Data.HashMap.Strict as HM
import Control.Concurrent
import Control.Exception

data Options = Options
  { optHost :: String
  , optPort :: Int
  , optDir :: Maybe FilePath
  }

options :: [OptDescr (Options -> Options)]
options  =
  [ Option ['p'] ["port"]
      (ReqArg (\x o -> o { optPort = read x }) "PORT") "port"
  , Option ['h'] ["host"]
      (ReqArg (\x o -> o { optHost = x }) "HOST") "host"
  , Option ['d'] ["dir"]
      (ReqArg (\x o -> o { optDir = Just x }) "DIR") "directory"
  ]

withOptions :: [String] -> (Options -> [String] -> IO ()) -> IO ()
withOptions args k = case getOpt Permute options args of
  (opts, xs, []) -> let o = foldl (flip id) (Options "127.0.0.1" 1886 Nothing) opts
    in k o xs
  (_, _, errs) -> ioError $ userError
    $ concat errs ++ usageInfo "Usage: lisztd [OPTION...] name" options

main :: IO ()
main = getArgs >>= \args -> withOptions args $ \(Options host port dir) _ -> do
  vServers <- newMVar HM.empty

  runServer host port $ \pending -> do
    let req = pendingRequest pending
    let (name', action) = BS.breakEnd (=='/') $ BS.drop 1 $ requestPath req
    let name = BS.take (BS.length name' - 1) name' -- Remove trailing "/"
    m <- takeMVar vServers
    app <- case HM.lookup name m of
      Just app -> app <$ putMVar vServers m
      Nothing -> do
        (_, _, app) <- openLisztServer
          (maybe "" dropTrailingPathSeparator dir ++ BS.unpack name)
          `onException` putMVar vServers m
        putMVar vServers $! HM.insert name app m
        return app
    app pending { pendingRequest = req { requestPath = action } }
