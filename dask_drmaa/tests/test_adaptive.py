from time import sleep, time

import pytest

from dask_drmaa import SGECluster
from dask_drmaa.adaptive import Adaptive
from distributed import Client
from distributed.utils_test import loop, inc


def test_adaptive_memory(loop):
    with SGECluster(scheduler_port=0) as cluster:
        adapt = Adaptive(cluster=cluster)
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 1, resources={'memory': 1e9})
            assert future.result() == 2
            assert len(cluster.scheduler.ncores) > 0
            r = list(cluster.scheduler.worker_resources.values())[0]
            assert r['memory'] > 1e9

            del future

            start = time()
            while client.ncores():
                sleep(0.3)
                assert time() < start + 10

            """ # TODO: jobs aren't shutting down when process endst
            start = time()
            while cluster.workers:
                sleep(0.1)
                assert time() < start + 60
            """


def test_adaptive_normal_tasks(loop):
    with SGECluster(scheduler_port=0) as cluster:
        adapt = Adaptive(cluster=cluster)
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 1)
            assert future.result() == 2
