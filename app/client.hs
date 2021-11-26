{-# LANGUAGE LambdaCase #-}
module Main where
import Database.Franz.URI
import Database.Franz.Client
import qualified Database.Franz.Contents as C

import Control.Monad
import Control.Concurrent.STM
import Data.Function (fix)
import Data.Functor.Identity
import Data.List (foldl')
import Data.String
import qualified Data.Vector as V
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as B
import Network.Socket (PortNumber)
import System.Environment
import System.IO
import System.Console.GetOpt
import System.Exit

parseHostPort :: String -> (FranzPath -> r) -> FilePath -> r
parseHostPort str k = case break (==':') str of
  (host, ':' : port) -> k . FranzPath host (read port)
  (host, _) -> k . FranzPath host 1886

data Options = Options
  { host :: String
  , prefix :: String
  , timeout :: Double
  , index :: Maybe B.ByteString
  , ranges :: [(Int, Int)]
  , beginning :: Maybe Int
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
  , Option "p" ["prefix"] (ReqArg (\str o -> o { prefix = str }) "PREFIX") "Archive prefix"
  , Option "i" ["index"] (ReqArg (\str o -> o { index = Just $ B.pack str }) "NAME") "Index name"
  ]

defaultOptions :: Options
defaultOptions = Options
  { host = "localhost"
  , timeout = 1e12
  , ranges = []
  , beginning = Nothing
  , index = Nothing
  , prefix = ""
  }

printBS :: Options -> C.Item -> IO ()
printBS o C.Item{C.payload = bs} = do
  BB.hPutBuilder stdout $ BB.word64LE $ fromIntegral $ B.length bs
  B.hPutStr stdout bs
  hFlush stdout

main :: IO ()
main = getOpt Permute options <$> getArgs >>= \case
  (fs, [name], []) -> do
    let o = foldl' (flip id) defaultOptions fs
    parseHostPort (host o) withConnection (prefix o) $ \conn -> do
      let name' = fromString name
      let timeout' = floor $ timeout o * 1000000
      let f = maybe BySeqNum ByIndex (index o)
      let req i j = Query name' (f i) (f j) AllItems
      forM_ (reverse $ ranges o) $ \(i, j) -> fetchSimple conn timeout' (req i j)
        >>= mapM_ (printBS o) . maybe [] C.toList
      forM_ (beginning o) $ \start -> flip fix start $ \self i -> do
        bss <- maybe [] C.toList <$> fetchSimple conn timeout' (req i i)
        mapM_ (printBS o) bss
        unless (null bss) $ self $ C.seqNo (last bss) + 1

  (_, _, es) -> do
    name <- getProgName
    die $ unlines ("franz [OPTIONS] STREAM" : es) ++ usageInfo name options
