{-# LANGUAGE LambdaCase #-}
module Main where

import Database.Liszt
import System.Environment

main :: IO ()
main = getArgs >>= \case
  [port, path] -> startServer (read port) path
  _ -> fail "Usage: itodenwad port prefix"
