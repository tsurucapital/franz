{-# LANGUAGE LambdaCase #-}
module Main where

import Database.Liszt.Network
import System.Environment
import System.Console.GetOpt
import System.Exit

data Options = Options
  { port :: Int
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
    startServer (port o) $ case args of
      path : _ -> path
      [] -> "."
  (_, _, es) -> do
    name <- getProgName
    die $ unlines es ++ usageInfo name options

