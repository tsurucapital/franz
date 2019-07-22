{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where
import Database.Franz
import Database.Franz.Network

import Control.Monad
import Control.Concurrent.STM
import Data.Function (fix)
import Data.Functor.Identity
import qualified Data.ByteString.Char8 as B
import Network.Socket (PortNumber)
import System.Environment
import System.IO
import System.Console.GetOpt
import System.Exit

parseHostPort :: String -> (String -> PortNumber -> r) -> r
parseHostPort str k = case break (==':') str of
  (host, ':' : port) -> k host (read port)
  (host, _) -> k host defaultPort

data Options = Options
  { host :: String
  , timeout :: Double
  , index :: Maybe B.ByteString
  , ranges :: [(Int, Int)]
  , beginning :: Maybe Int
  , prefixLength :: Bool
  }

readOffset :: String -> Int
readOffset ('_' : n) = -read n
readOffset n = read n

options :: [OptDescr (Options -> Options)]
options = [Option "h" ["host"] (ReqArg (\str o -> o { host = str }) "HOST:PORT") "stream input"
  , Option "r" ["range"] (ReqArg (\str o -> o { ranges = case break (==':') str of
      (begin, ':' : end) -> (readOffset begin, readOffset end) : ranges o
      _ -> (readOffset str, readOffset str) : ranges o
      }) "FROM:TO") "ranges"
  , Option "b" ["begin"] (ReqArg (\str o -> o { beginning = Just $! readOffset str }) "pos") "get all the contents from this position"
  , Option "t" ["timeout"] (ReqArg (\str o -> o { timeout = read str }) "SECONDS") "Timeout"
  , Option "i" ["index"] (ReqArg (\str o -> o { index = Just $ B.pack str }) "NAME") "Index name"
  , Option "" ["prefix-length"] (NoArg (\o -> o { prefixLength = True })) "Prefix payloads by their lengths"
  ]

defaultOptions :: Options
defaultOptions = Options
  { host = "localhost"
  , timeout = 1e12
  , ranges = []
  , beginning = Nothing
  , index = Nothing
  , prefixLength = False
  }

printBS :: Options -> (a, b, B.ByteString) -> IO ()
printBS o (_, _, bs) = do
  when (prefixLength o) $ print $ B.length bs
  B.hPutStr stdout bs
  hFlush stdout

main :: IO ()
main = getOpt Permute options <$> getArgs >>= \case
  (fs, [name], []) -> do
    let o = foldl (flip id) defaultOptions fs
    parseHostPort (host o) withConnection mempty $ \conn -> do
      let name' = B.pack name
      let timeout' = floor $ timeout o * 1000000
      let f = maybe BySeqNum ByIndex (index o)
      let req i j = Query name' (f i) (f j) AllItems
      forM_ (reverse $ ranges o) $ \(i, j) -> fetchSimple conn timeout' (req i j)
        >>= mapM_ (printBS o)
      forM_ (beginning o) $ \start -> flip fix start $ \self i -> do
        bss <- fetchSimple conn timeout' (req i i)
        mapM_ (printBS o) bss
        unless (null bss) $ self $ let (j, _, _) = last bss in j + 1

  (_, _, es) -> do
    name <- getProgName
    die $ unlines ("franz [OPTIONS] STREAM" : es) ++ usageInfo name options
