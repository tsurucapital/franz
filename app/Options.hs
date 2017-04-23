module Options where

import System.Console.GetOpt

hostPort :: [OptDescr ((String, Int) -> (String, Int))]
hostPort =
  [ Option ['p'] ["port"]
      (ReqArg (\x (h, _) -> (h, read x)) "PORT") "port"
  , Option ['h'] ["host"]
      (ReqArg (\x (_, p) -> (x, p)) "HOST") "host"
  ]

withHostPort :: [String] -> (String -> Int -> [String] -> IO ()) -> IO ()
withHostPort args k = case getOpt Permute hostPort args of
  (opts, xs, []) -> let (h, p) = foldl (flip id) ("127.0.0.1", 6000) opts
    in k h p xs
  (_, _, errs) -> ioError $ userError
    $ concat errs ++ usageInfo "Usage: liszt read [OPTION...]" hostPort
