#!/bin/sh
set -E
docker build --rm -t local/raft-peer -f Dockerfile .
