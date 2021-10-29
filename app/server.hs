module Main where

import Database.Franz.Reader (FranzPrefix(..))
import Database.Franz.Server
import Options.Applicative

options :: Parser Settings
options = Settings
  <$> option auto (long "reap-interval" <> value 60 <> metavar "SECONDS" <> help "Stream reaping interval")
  <*> option auto (long "stream-lifetime" <> value 3600 <> metavar "SECONDS" <> help "Number of seconds to leave stream open with no readers")
  <*> option auto (long "port" <> value defaultPort <> help "Port number")
  <*> prefixOption (long "live" <> value "." <> metavar "DIR" <> help "Live prefix")
  <*> optional (prefixOption (long "archive" <> metavar "DIR" <> help "Search for squashfs archives in this directory. If none found, search for live streams instead."))
  <*> prefixOption (long "mount" <> value "/tmp/franz" <> metavar "DIR" <> help "Mount prefix")

prefixOption :: Mod OptionFields FilePath -> Parser FranzPrefix
prefixOption = fmap FranzPrefix . strOption

main :: IO ()
main = execParser (info options mempty) >>= startServer
