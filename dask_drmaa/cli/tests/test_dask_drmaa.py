from distributed.utils_test import popen
from distributed import Client
from distributed.utils_test import loop

def test_dask_drmaa(loop):
    with popen(['dask-drmaa', '2']) as proc:
        with Client('127.0.0.1:8786', loop=loop) as client:
            assert len(client.ncores()) == 2
