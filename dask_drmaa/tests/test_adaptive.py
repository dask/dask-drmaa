from dask_drmaa import SGECluster
from dask_drmaa.adaptive import Adaptive
from distributed import Client
from distributed.utils_test import loop, inc

from time import sleep, time

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

            """
            start = time()
            while cluster.workers:
                sleep(0.1)
                assert time() < start + 10
            """

def test_sge_adaptive_worker_cleanup(loop):
    """Test that cluster workers are cleaned up after an appropriate 
         waiting time. In this case, SGE will clean things up after 20 seconds.
    """
    def long_running_func(time, num):
        sleep(time)
        return num + 1
    
    with SGECluster(scheduler_port=0, max_runtime='0:0:15') as cluster:
        adapt = Adaptive(cluster=cluster)
        with Client(cluster, loop=loop) as client:
            future = client.submit(long_running_func, 5, 3, resources={'memory': 1e9})
            while future.status == 'pending':
                sleep(1)
                
            running_workers = [jid for jid in cluster.workers if cluster.jobStatus(jid) in ("running", "queued_active")]
            assert len(running_workers) > 0
            assert future.result() == 4
            
            start = time()
            #If the workers are done, ignore them. The scheduler
            #  might keep a record of them around for a while in case someone
            #  wants to check on them.
            while running_workers:
                print(running_workers)
                sleep(1)
                assert time() < start + 30
                running_workers = [jid for jid in cluster.workers if cluster.jobStatus(jid) in ("running", "queued_active")]

            del future
