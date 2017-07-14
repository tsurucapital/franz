# liszt

Liszt is a publish & subscribe persistent messaging system.

`lisztd` starts a server.

```
$ lisztd
```

Consume the stream from the beginning:

```
$ liszt read localhost foo
```

Send data from stdin to the liszt:

```
$ liszt write localhost foo
```
