#!/bin/bash

# start sge
sudo service gridengine-exec restart

sleep 4

sudo service gridengine-exec restart

python -m SimpleHTTPServer 8888
