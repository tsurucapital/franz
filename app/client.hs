{-# LANGUAGE ApplicativeDo #-}
module Main where
import Database.Franz.Internal.URI
import Database.Franz.Client
import qualified Database.Franz.Contents as C

import Control.Monad
import Data.Function (fix)
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as B
import System.IO
import Text.Read (readEither)
import Options.Applicative

printBS :: C.Item -> IO ()
printBS C.Item{C.payload = bs} = do
  BB.hPutBuilder stdout $ BB.word64LE $ fromIntegral $ B.length bs
  B.hPutStr stdout bs
  hFlush stdout

main :: IO ()
main = join $ execParser (info app mempty)

parseRange :: String -> Either String (Int, Int)
parseRange st = case break (==':') st of
  (begin, ':' : end) -> (,) <$> readEither begin <*> readEither end
  _ -> readEither st >>= \x -> pure (x, x)

app :: Parser (IO ())
app = do
  ranges <- many $ option (eitherReader parseRange)
    $ short 'r' <> long "range" <> metavar "FROM:TO" <> help "range of seqnos to read"
  path <- argument (eitherReader toFranzPath) $ metavar "URL" <> help "Path or URL to the directory"
  timeout <- fmap (floor . (* 1000000)) $ option auto
    $ long "timeout" <> value (30 :: Double) <> help "Timeout in seconds"
  follow <- switch $ long "follow" <> help "Follow the stream"
  stream <- strArgument $ metavar "NAME" <> help "Stream name"

  pure $ withConnection path $ \conn -> do
    let req i j = Query stream (BySeqNum i) (BySeqNum j) AllItems

    -- Query data specified in --range
    forM_ ranges $ \(i, j) -> fetchSimple conn timeout (req i j)
      >>= mapM_ printBS . maybe [] C.toList

    let start = case ranges of
          [] -> 0
          xs -> snd (last xs) + 1

    when follow $ flip fix start $ \self i -> do
      bss <- maybe [] C.toList <$> fetchSimple conn timeout (req i i)
      mapM_ printBS bss
      unless (null bss) $ self $ C.seqNo (last bss) + 1
