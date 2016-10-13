from time import sleep, time

from dask_drmaa import DRMAACluster
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
