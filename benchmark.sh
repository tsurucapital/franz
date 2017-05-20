#!/bin/bash

stack exec lisztd benchmark +RTS -N4 &

LISZTD=$!

sleep 0.1

stack exec liszt subscribe localhost > /dev/null &

CONSUMER=$!

seq 0 99999 | time stack exec liszt write localhost

kill $CONSUMER
kill $LISZTD
rm -r benchmark.indices
rm -r benchmark.payload
