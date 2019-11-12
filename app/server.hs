{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where

import Data.List (foldl')
import Database.Franz.Network
import System.Environment
import System.Console.GetOpt
import System.Exit
import Network.Socket (PortNumber)

data Options = Options
  { port :: PortNumber
  , reaperInterval :: Double
  , streamLife :: Double
  }

defaultOptions :: Options
defaultOptions = Options
  { port = defaultPort
  , reaperInterval = 60
  , streamLife = 3600
  }

options :: [OptDescr (Options -> Options)]
options = [Option "p" ["port"] (ReqArg (\e o -> o { port = read e }) "NUM") "port number"
  , Option "l" ["life"] (ReqArg (\e o -> o { streamLife = read e }) "SECS") "lifespan of streams"]

main :: IO ()
main = getOpt Permute options <$> getArgs >>= \case
  (fs, args, []) -> do
    let Options{..} = foldl' (flip id) defaultOptions fs
    let start = startServer reaperInterval streamLife port
    case args of
      path : apath : _ -> start path (Just apath)
      path : _ -> start path Nothing
      [] -> start "." Nothing
  (_, _, es) -> do
    name <- getProgName
    die $ unlines ("franzd PATH [ARCHIVE_PATH]" : es) ++ usageInfo name options
