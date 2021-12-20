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