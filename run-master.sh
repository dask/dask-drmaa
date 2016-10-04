#!/bin/bash


# start sge
sudo service gridengine-master restart

while ! ping -c1 slave_one &>/dev/null; do :; done

qconf -Msconf /scheduler.txt
qconf -Ahgrp /hosts.txt
qconf -Aq /queue.txt

qconf -ah slave_one.daskdrmaa_default
qconf -ah slave_two.daskdrmaa_default
qconf -ah slave_three.daskdrmaa_default

qconf -as $HOSTNAME
bash add_worker.sh dask.q slave_one.daskdrmaa_default 4

sudo service gridengine-master restart

python -m SimpleHTTPServer 8888
