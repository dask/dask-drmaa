from time import sleep

import pytest

from dask_drmaa import SGECluster
from distributed import Client
from distributed.utils_test import loop

def test_sge_memory(loop):
    with SGECluster(scheduler_port=0) as cluster:
        cluster.start_workers(2, memory=3e9, memory_fraction=0.5)
        with Client(cluster, loop=loop) as client:
            while len(cluster.scheduler.ncores) < 2:
                sleep(0.1)

            assert all(info['memory_limit'] == 1.5e9
                       for info in cluster.scheduler.worker_info.values())


def test_sge_cpus(loop):
    with SGECluster(scheduler_port=0) as cluster:
        cluster.start_workers(1, cpus=2)
        with Client(cluster, loop=loop) as client:
            while len(cluster.scheduler.ncores) < 1:
                sleep(0.1)

            assert list(cluster.scheduler.ncores.values()) == [2]
