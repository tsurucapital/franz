{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedStrings #-}
module Database.Franz.Network
  (startServer
  , Connection
  , Directory(..)
  , withConnection
  , connect
  , disconnect
  , fetch) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Concurrent.STM
import Database.Franz
import Data.Serialize
import Data.Serialize.Get
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import GHC.Generics (Generic)
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.Directory
import System.FilePath
import System.IO
import System.Process

data Response = ResponseSuccess !Int
    | ResponseError !FranzError
    deriving (Show, Generic)
instance Serialize Response

respond :: FranzReader -> S.Socket -> IO ()
respond env conn = do
  msg <- SB.recv conn 4096
  (payloadHandle, offsets) <- case decode msg of
    Left _ -> throwIO MalformedRequest
    Right a -> handleRequest env a
  SB.sendAll conn $ encode $ ResponseSuccess $ length offsets
  forM_ offsets $ \(i, xs, pos, len) -> do
    SB.sendAll conn $ encode (i, HM.toList xs, len)
    SF.sendFile' conn payloadHandle (fromIntegral pos) (fromIntegral len)

startServer :: Int
    -> FilePath -- live prefix
    -> Maybe FilePath -- archive prefix
    -> IO ()
startServer port prefix aprefix = withFranzReader prefix $ \env -> do
  vMountCount <- newTVarIO HM.empty
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock 2
    forever $ do
      (conn, _) <- S.accept sock
      forkFinally (do
        decode <$> SB.recv conn 4096 >>= \case
          Left _ -> throwIO MalformedRequest
          Right (Archive path) | Just apath <- aprefix -> do
            let src = apath </> B.unpack path
            let dest = prefix </> B.unpack path
            join $ atomically $ do
              m <- readTVar vMountCount
              case HM.lookup path m of
                Nothing -> do
                  writeTVar vMountCount $ HM.insert path 1 m
                  return $ do
                    b <- doesFileExist src
                    when b $ do
                      createDirectoryIfMissing True dest
                      callProcess "squashfuse" [src, dest]
                Just n -> fmap pure $ writeTVar vMountCount $ HM.insert path (n + 1) m
            SB.sendAll conn "READY"
            forever (respond env conn)
              `finally` do
                join $ atomically $ do
                  m <- readTVar vMountCount
                  case HM.lookup path m of
                    Just 1 -> do
                      writeTVar vMountCount $ HM.delete path m
                      return $ callProcess "fusermount" ["-u", dest]
                    Just n -> do
                      writeTVar vMountCount $ HM.insert path (n - 1) m
                      pure (pure ())
                    Nothing -> pure (pure ())
          Right _ -> do
            SB.sendAll conn "READY"
            forever $ respond env conn
        )
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ encode $ ResponseError e
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

newtype Connection = Connection (MVar S.Socket)

withConnection :: String -> Int -> Directory -> (Connection -> IO r) -> IO r
withConnection host port dir = bracket (connect host port dir) disconnect

data Directory = Live | Archive !B.ByteString deriving (Show, Generic)
instance Serialize Directory

connect :: String -> Int -> Directory -> IO Connection
connect host port dir = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  SB.sendAll sock $ encode dir
  resp <- SB.recv sock 4096
  case resp of
    "READY" -> Connection <$> newMVar sock
    msg -> case decode msg of
      Right (ResponseError e) -> throw e
      e -> fail $ "connect: Unexpected response: " ++ show e

disconnect :: Connection -> IO ()
disconnect (Connection sock) = takeMVar sock >>= S.close

fetch :: Connection -> Request -> IO [(Int, IndexMap Int, B.ByteString)]
fetch (Connection vsock) req = modifyMVar vsock $ \sock -> do
  SB.sendAll sock $ encode req
  let go (Done (Right a) _) = return (sock, a)
      go (Done (Left e) _) = throwIO e
      go (Partial cont) = do
        bs <- SB.recv sock 4096
        if B.null bs then go $ cont "" else go $ cont bs
      go (Fail str _) = fail $ show req ++ ": " ++ str
  bs <- SB.recv sock 4096
  go $ flip runGetPartial bs $ get >>= \case
    ResponseSuccess n -> fmap Right $ replicateM n
      $ (,,) <$> get <*> fmap HM.fromList get <*> get
    ResponseError e -> return $ Left e
