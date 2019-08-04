#!/usr/bin/env bash

docker build -t igor:daemon -f Dockerfile.daemon ./
docker build -t igor:gateway -f Dockerfile.gateway ./
docker build -t igor:scheduler -f Dockerfile.scheduler ./