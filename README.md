# Franz

## start a server

```
franzd /var/franz/live /var/franz/archive -p PORT
```

or

```
docker run -p 1886:1886 -v /u/tsuru/franz/live:/live -v /u/tsuru/franz/archive:/archive fumieval/franz:latest
```

## reading

Read elements from 0th to 9th

```
franz test -r 0:9
```

Follow a stream from the first element

```
franz test -b _1
``
