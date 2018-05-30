module Database.Liszt.Network
  (startServer
  , Connection
  , withConnection
  , connect
  , disconnect
  , fetch) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Database.Liszt
import Data.Binary
import Data.Binary.Get
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Network.Socket.SendFile.Handle as SF
import qualified Network.Socket.ByteString as SB
import qualified Network.Socket as S
import System.IO

respond :: LisztReader -> S.Socket -> IO ()
respond env conn = do
  msg <- BL.fromStrict <$> SB.recv conn 4096
  (payloadHandle, offsets) <- case decodeOrFail msg of
    Left _ -> throwIO MalformedRequest
    Right (_, _, a) -> handleRequest env a
  SB.sendAll conn $ BL.toStrict $ encode $ length offsets
  forM_ offsets $ \(i, xs, pos, len) -> do
    SB.sendAll conn $ BL.toStrict $ encode (i, HM.toList xs, len)
    SF.sendFile' conn payloadHandle (fromIntegral pos) (fromIntegral len)

startServer :: Int -> FilePath -> IO ()
startServer port path = withLisztReader path $ \env -> do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICHOST, S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just "0.0.0.0") (Just $ show port)
  bracket (S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)) S.close $ \sock -> do
    S.setSocketOption sock S.ReuseAddr 1
    S.setSocketOption sock S.NoDelay 1
    S.bind sock $ S.SockAddrInet (fromIntegral port) (S.tupleToHostAddress (0,0,0,0))
    S.listen sock 2
    forever $ do
      (conn, _) <- S.accept sock
      forkFinally (forever $ respond env conn)
        $ \result -> do
          case result of
            Left ex -> case fromException ex of
              Just e -> SB.sendAll conn $ B.pack $ show (e :: LisztError)
              Nothing -> hPutStrLn stderr $ show ex
            Right _ -> return ()
          S.close conn

newtype Connection = Connection S.Socket

withConnection :: String -> Int -> (Connection -> IO r) -> IO r
withConnection host port = bracket (connect host port) disconnect

connect :: String -> Int -> IO Connection
connect host port = do
  let hints = S.defaultHints { S.addrFlags = [S.AI_NUMERICSERV], S.addrSocketType = S.Stream }
  addr:_ <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
  sock <- S.socket (S.addrFamily addr) (S.addrSocketType addr) (S.addrProtocol addr)
  S.connect sock $ S.addrAddress addr
  return $ Connection sock

disconnect :: Connection -> IO ()
disconnect (Connection sock) = S.close sock

fetch :: Connection -> Request -> IO [(Int, IndexMap Int, B.ByteString)]
fetch (Connection sock) req = do
  SB.sendAll sock $ BL.toStrict $ encode req
  go $ runGetIncremental $ get
    >>= \n -> replicateM n ((,,) <$> get <*> fmap HM.fromList get <*> get)
  where
    go (Done _ _ a) = return a
    go (Partial cont) = do
      bs <- SB.recv sock 4096
      if B.null bs then go $ cont Nothing else go $ cont $ Just bs
    go (Fail _ _ str) = fail $ show req ++ ": " ++ str
