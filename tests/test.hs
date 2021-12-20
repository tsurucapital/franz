{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
import Control.Monad
import Database.Franz.Writer
import Database.Franz.Client
import Database.Franz.Contents as C
import qualified Data.ByteString.FastBuilder as BB
import Data.Functor.Identity
import System.IO.Temp
import Data.IORef

main :: IO ()
main = withSystemTempDirectory "franz-test" $ \path -> do
    withWriter (Identity "") path
        $ \h -> forM_ ([0..9] :: [Int])
        $ \i -> write h (Identity (fromIntegral i * 10)) (BB.intDec i)

    numFails <- newIORef (0 :: Int)

    withConnection (LocalFranzPath path) $ \conn -> do
        let test :: RequestType -> ItemRef -> ItemRef -> [Item] -> IO ()
            test ty i j expected = do
                putStr $ unwords ["test", show ty, showsPrec 11 i "", showsPrec 11 j "", show expected, ": "]
                fetchSimple conn 500000 (Query "" i j ty) >>= \case
                    Nothing -> error "timeout"
                    Just xs
                        | C.toList xs == expected -> putStrLn "ok"
                        | otherwise -> do
                            modifyIORef' numFails (+1)
                            putStrLn $ "got " <> show (C.toList xs)

        test AllItems (BySeqNum 1) (BySeqNum 3) [Item {seqNo = 1, indices = [10], payload = "1"},Item {seqNo = 2, indices = [20], payload = "2"},Item {seqNo = 3, indices = [30], payload = "3"}]
        test AllItems (ByIndex "" 15) (ByIndex "" 15) []
        test AllItems (ByIndex "" 15) (ByIndex "" 25) [Item {seqNo = 2, indices = [20], payload = "2"}]
        test AllItems (ByIndex "" 10) (ByIndex "" 20) [Item {seqNo = 1, indices = [10], payload = "1"},Item {seqNo = 2, indices = [20], payload = "2"}]
        test AllItems (ByIndex "" 15) (ByIndex "" 20) [Item {seqNo = 2, indices = [20], payload = "2"}]

        test FirstItem (BySeqNum 1) (BySeqNum 5) [Item {seqNo = 1, indices = [10], payload = "1"}]
        test FirstItem (ByIndex "" 10) (ByIndex "" 45) [Item {seqNo = 1, indices = [10], payload = "1"}]
        test FirstItem (ByIndex "" 15) (ByIndex "" 45) [Item {seqNo = 2, indices = [20], payload = "2"}]
        test FirstItem (ByIndex "" 10) (ByIndex "" 10) [Item {seqNo = 1, indices = [10], payload = "1"}]
        test FirstItem (ByIndex "" 15) (ByIndex "" 15) []

        test LastItem (BySeqNum 1) (BySeqNum 5) [Item {seqNo = 5, indices = [50], payload = "5"}]
        test LastItem (ByIndex "" 10) (ByIndex "" 45) [Item {seqNo = 4, indices = [40], payload = "4"}]
        test LastItem (ByIndex "" 15) (ByIndex "" 50) [Item {seqNo = 5, indices = [50], payload = "5"}]
        test LastItem (ByIndex "" 10) (ByIndex "" 10) [Item {seqNo = 1, indices = [10], payload = "1"}]
        test LastItem (ByIndex "" 15) (ByIndex "" 15) []

    readIORef numFails >>= \case
        0 -> putStrLn "All good"
        _ -> fail "Failed"