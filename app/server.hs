{-# LANGUAGE LambdaCase, RecordWildCards #-}
module Main where

import Database.Franz.Server
import Options.Applicative

options :: Parser Settings
options = Settings
  <$> option auto (long "reap-interval" <> value 60 <> metavar "SECONDS" <> help "Stream reaping interval")
  <*> option auto (long "stream-lifetime" <> value 3600 <> metavar "SECONDS" <> help "Number of seconds to leave stream open with no readers")
  <*> option auto (long "port" <> value defaultPort <> help "Port number")
  <*> strOption (long "live" <> value "." <> metavar "DIR" <> help "Live prefix")
  <*> optional (strOption (long "archive" <> metavar "DIR" <> help "Search for squashfs archives in this directory. If none found, search for live streams instead."))
  <*> strOption (long "mount" <> value "/tmp/franz" <> metavar "DIR" <> help "Mount prefix")

main :: IO ()
main = execParser (info options mempty) >>= startServer
