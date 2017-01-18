from time import sleep, time

import pytest

from dask_drmaa import SGECluster
from dask_drmaa.adaptive import Adaptive
from distributed import Client
from distributed.utils_test import loop, inc, slowinc


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


@pytest.mark.parametrize('interval', [50, 1000])
def test_dont_over_request(loop, interval):
    with SGECluster(scheduler_port=0) as cluster:
        adapt = Adaptive(cluster=cluster)
        with Client(cluster, loop=loop) as client:
            future = client.submit(inc, 1)
            assert future.result() == 2
            assert len(cluster.scheduler.workers) == 1

            for i in range(5):
                sleep(0.2)
                assert len(cluster.scheduler.workers) == 1


def test_request_more_than_one(loop):
    with SGECluster(scheduler_port=0) as cluster:
        adapt = Adaptive(cluster=cluster)
        with Client(cluster, loop=loop) as client:
            futures = client.map(slowinc, range(10000), delay=0.2)
            while len(cluster.scheduler.workers) < 3:
                sleep(0.1)
