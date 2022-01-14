# v0.5.3

* It now delays a response if it fails to resolve the range specified by `ByIndex`

# v0.5.2

* Fixed the undesirable behaviour that causes an empty response when `BySeqNum (-1)` is specified
* Expanded `Reconnect` so that the query that caused a reconnection is visible

# v0.5.1

* Fixed the bug adding all the following payloads in the result of `index`
* Added `FirstItem` to `RequestType`

# v0.5

* Renamed internal modules under `Internal` prefix
* Renamed `Database.Franz.Reconnect` to `Database.Franz.Client.Reconnect`

# v0.4

* Added `StreamName`
    * `reqStream` now has a type `StreamName`
* Added `Database.Franz.Reconnect`
* Added `Database.Franz.URI`
* Supported reading local franz streams
* Renamed `Database.Franz` to `Database.Franz.Writer`
* Reworked the `Contents` type
