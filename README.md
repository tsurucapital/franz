# Franz

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

```
franzd --live /path/to/live --archive /path/to/archive
```

You can obtain a `Connection` to a remote franz file with `withConnection`.
It tries to mount a squashfs image at `path`. This is shared between connections, and unmounts when the last client closes the connection.

```haskell
withConnection :: (MonadIO m, MonadMask m)
  => String -- host
  -> Int -- port
  -> ByteString -- path
  -> (Connection -> m r) -> m r
```

`fetch` returns a list of triples of offsets, tags, and payloads.

```haskell
data RequestType = AllItems | LastItem deriving (Show, Generic)

data ItemRef = BySeqNum !Int -- ^ sequential number
  | ByIndex !B.ByteString Int -- ^ index name and value

data Query = Query
  { reqStream :: !B.ByteString
  , reqFrom :: !ItemRef -- ^ name of the index to search
  , reqTo :: !ItemRef -- ^ name of the index to search
  , reqType :: !RequestType
  } deriving (Show, Generic)

type SomeIndexMap = HM.HashMap B.ByteString Int64

type Contents = [(Int, SomeIndexMap, B.ByteString)]

-- | When it is 'Right', it blocks until the content is available on the server.
type Response = Either Contents (STM Contents)

fetch :: Connection
  -> Query
  -> (STM Response -> IO r)
  -- ^ running the STM action blocks until the response arrives
  -> IO r
```

## franz CLI: reading

Read 0th to 9th elements

```
franz test -r 0:9
```

Follow a stream

```
franz test -b _1
``
