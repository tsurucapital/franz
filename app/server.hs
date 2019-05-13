{-# LANGUAGE LambdaCase #-}
module Main where

import Database.Franz.Network
import System.Environment
import System.Console.GetOpt
import System.Exit
import Network.Socket (PortNumber)

data Options = Options
  { port :: PortNumber
  }

defaultOptions :: Options
defaultOptions = Options
  { port = 1886
  }

options :: [OptDescr (Options -> Options)]
options = [Option "p" ["port"] (ReqArg (\e o -> o { port = read e }) "NUM") "port number"]

main :: IO ()
main = getOpt Permute options <$> getArgs >>= \case
  (fs, args, []) -> do
    let o = foldl (flip id) defaultOptions fs
    case args of
      path : apath : _ -> startServer (port o) path (Just apath)
      path : _ -> startServer (port o) path Nothing
      [] -> startServer (port o) "." Nothing
  (_, _, es) -> do
    name <- getProgName
    die $ unlines es ++ usageInfo name options
