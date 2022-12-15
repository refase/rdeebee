#!/bin/bash
docker build -t ratnadeepb/rdb-server -f Dockerfile ../../
docker push ratnadeepb/rdb-server:latest