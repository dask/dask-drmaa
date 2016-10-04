#!/usr/bin/env bash

set -e
set -x

# Update package for ubuntu
sudo apt-get -qq update
sudo apt-get install -y --force-yes -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" docker-engine

# Start service
sudo service docker stop
sudo service docker start

# Setup/update docker compose
curl -L https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-`uname -s`-`uname -m` > docker-compose
chmod +x docker-compose
sudo mv docker-compose /usr/local/bin
