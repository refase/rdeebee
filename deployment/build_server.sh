#!/bin/bash
docker build -t ratnadeepb/rdb-server -f Dockerfile-server ../
docker push ratnadeepb/rdb-server:latest