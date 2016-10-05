#!/bin/bash
docker-compose up -d
while ! docker exec -it sge_master qhost | grep lx26-amd64 &>/dev/null; 
  do 
    echo "Waiting for SGE slots to become available"; 
    sleep 1
  done
echo "SGE properly configured"
