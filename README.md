# Franz

![Haskell CI](https://github.com/fumieval/franz/workflows/Haskell%20CI/badge.svg)

Franz is an append-only container format, forked from liszt.

Each stream is stored as a pair of concatenated payloads with an array of their
byte offsets.

## Design requirements

* The writer must be integrated so that no server failure blocks the application.
* There's a way to archive streams into one file.
* There's a way to fetch data in a period of time efficiently.
    * In particular, the server should be able to search by timestamps, rather than performing binary search by the client.
* The server must not take too long to restart.

## Usecase

* Instances of franzd are running on a remote server and a local gateway.
* The application produces franz files locally using the writer API.
* On the local gateway, a proxy connects to the remote server and downsamples the file.
* Clients can connect to the gateway. When needed, they may also connect directly to the remote server.

## Format details

The on-disk representation of a franz stream comprises the following files:

* `payloads`: concatenated payloads
* `offsets`: A sequence of N-tuples of 64-bit little endian integers representing
    * 0th: byte offsets of payloads
    * nth, n ∈ [1..N]: the value of nth index, where N is the number of index names
* `indices`: Line-separated list of index names. An index represents a 64 bit little-endian integer attached to a payload.

A stream is stored as a directory containing the files above.

The Franz reader also supports a squashfs image, provided that the content is a valid franz stream.

## franzd

franzd is a read-only server which follows franz files and gives access on wire.
Where to look for streams can be specified as a command-line argument, separately for live streams and squashfs images.

Each stream is stored as a pair of concatenated payloads with an array of their
byte offsets.

```
franzd --live /path/to/live --archive /path/to/archive
```

## Why not Kafka

- None of us want to debug/contribute to kafka.
- Trying to read from a stream creates the stream (this is a problem due to the way we name our streams and rely on `latest`)
- Can't delete a stream as long as there is a reader existing
- Lack of understanding of it (but there is a lot of good documentation out there. [recommended][1])
- Kafka takes a long time to start up after an abnormal shutdown on the server side
- Supports clustering but sometimes makes the reliability of the whole system worse

[1]: https://kafka.apache.org/documentation/#design

## Client API

`Database.Franz.Client` exposes the client API.

You can obtain a `Connection` to a remote franz file with `withConnection`.
It tries to mount a squashfs image at `path`. This is shared between connections, and unmounts when the last client closes the connection.

```haskell
toFranzPath :: String -> Either String FranzPath

withConnection :: (MonadIO m, MonadMask m)
  => FranzPath
  -> (Connection -> m r) -> m r
```

```haskell
data RequestType = AllItems | LastItem deriving (Show, Generic)

data ItemRef = BySeqNum !Int -- ^ sequential number
  | ByIndex !IndexName Int -- ^ index name and value

data Query = Query
  { reqStream :: !StreamName
  , reqFrom :: !ItemRef -- ^ name of the index to search
  , reqTo :: !ItemRef -- ^ name of the index to search
  , reqType :: !RequestType
  } deriving (Show, Generic)

-- | When it is 'Right', it blocks until the content is available on the server.
type Response = Either Contents (STM Contents)

fetch :: Connection
  -> Query
  -> (STM Response -> IO r)
  -- ^ running the STM action blocks until the response arrives
  -> IO r
```

`Contents` is a datatype containing triples of sequential numbers, indices and payloads. It is recommended to import `Database.Franz.Contents` qualified.

```haskell
data Contents

data Item = Item
  { seqNo :: !Int
  , indices :: !(U.Vector Int64)
  , payload :: !B.ByteString
  } deriving (Show, Eq)

toList :: Contents -> [Item]
```

## Writer API

`Database.Franz.Writer` provides the writer interface.

```haskell
withWriter :: Foldable f
  => f String
  -> FilePath
  -> (WriterHandle f -> IO a)
  -> IO a
```

`withWriter` acquires a handle. The `f String` parameter represents a list of index names.

```haskell
write :: Foldable f
  => WriterHandle f
  -> f Int64 -- ^ index values
  -> Builder -- ^ payload
  -> IO Int
flush :: WriterHandle f -> IO ()
```

`write` appends a payload to the stream. `f Int64` is the list of index values, and it has to have the same length as the one you specified in `withWriter`. Changes will be written to disk whenever the buffer gets full or you call `flush`.

If you don't need the index mechanism, you can use `Database.Franz.Writer.Simple` instead.