#!/bin/bash
docker build -t ratnadeepb/rdb-client -f Dockerfile-client ../
docker push ratnadeepb/rdb-client:latest