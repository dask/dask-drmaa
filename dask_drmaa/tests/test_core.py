from time import sleep, time

from dask_drmaa import DRMAACluster, SGECluster
from distributed import Client
from distributed.utils_test import loop


def test_simple(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        with Client(cluster, loop=loop) as client:
            cluster.start_workers(2)
            future = client.submit(lambda x: x + 1, 1)
            assert future.result() == 2

            cluster.stop_workers(cluster.workers)

            start = time()
            while client.ncores():
                sleep(0.2)
                assert time() < start + 60

            assert not cluster.workers


def test_str(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        cluster.start_workers(2)
        assert 'DRMAACluster' in str(cluster)
        assert 'DRMAACluster' in repr(cluster)
        assert '2' in str(cluster)
        assert '2' in repr(cluster)
        1 + 1


def test_sge_memory(loop):
    with SGECluster(scheduler_port=0) as cluster:
        cluster.start_workers(2, memory=2e9)
        with Client(cluster, loop=loop) as client:
            while len(cluster.scheduler.ncores) < 2:
                sleep(0.1)

            assert all(1e9 < info['memory_limit'] < 2e9
                       for info in cluster.scheduler.worker_info.values())


def test_sge_cpus(loop):
    with SGECluster(scheduler_port=0) as cluster:
        cluster.start_workers(1, cpus=2)
        with Client(cluster, loop=loop) as client:
            while len(cluster.scheduler.ncores) < 1:
                sleep(0.1)

            assert list(cluster.scheduler.ncores.values()) == [2]
