from __future__ import print_function

import os
import shutil
import sys
import tempfile
from time import sleep, time

import pytest

from dask_drmaa import DRMAACluster
from dask_drmaa.core import make_job_script, worker_bin_path
from distributed import Client
from distributed.utils_test import loop, inc
from distributed.utils import tmpfile


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

    assert not os.path.exists(cluster.script)


def test_str(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        cluster.start_workers(2)
        assert 'DRMAACluster' in str(cluster)
        assert 'DRMAACluster' in repr(cluster)
        assert '2' in str(cluster)
        assert '2' in repr(cluster)
        1 + 1


def test_pythonpath():
    tmpdir = tempfile.mkdtemp(prefix='test_drmaa_pythonpath_', dir='.')
    try:
        with open(os.path.join(tmpdir, "bzzz_unlikely_module_name.py"), "w") as f:
            f.write("""if 1:
                def f():
                    return 5
                """)

        def func():
            import bzzz_unlikely_module_name
            return bzzz_unlikely_module_name.f()

        with DRMAACluster(scheduler_port=0,
                          preexec_commands=['export PYTHONPATH=%s:PYTHONPATH' % tmpdir],
                          ) as cluster:
            with Client(cluster) as client:
                cluster.start_workers(2)
                x = client.submit(func)
                assert x.result() == 5

    finally:
        shutil.rmtree(tmpdir)


def test_job_name_as_name(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        cluster.start_workers(2)
        while len(cluster.scheduler.workers) < 2:
            sleep(0.1)

        names = {cluster.scheduler.worker_info[w]['name']
                 for w in cluster.scheduler.workers}

        assert names == set(cluster.workers)


def test_multiple_overlapping_clusters(loop):
    with DRMAACluster(scheduler_port=0) as cluster_1:
        cluster_1.start_workers(1)
        with Client(cluster_1, loop=loop) as client_1:
            with DRMAACluster(scheduler_port=0) as cluster_2:
                cluster_2.start_workers(1)
                with Client(cluster_2, loop=loop) as client_2:
                    future_1 = client_1.submit(inc, 1)
                    future_2 = client_2.submit(inc, 2)

                    assert future_1.result() == 2
                    assert future_2.result() == 3


def test_stop_single_worker(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        with Client(cluster, loop=loop) as client:
            cluster.start_workers(2)
            future = client.submit(lambda x: x + 1, 1)
            assert future.result() == 2
            while len(client.ncores()) < 2:
                sleep(0.1)

            a, b = cluster.workers
            local_dir = client.run(lambda dask_worker: dask_worker.local_dir,
                                   workers=[a])[a]
            assert os.path.exists(local_dir)

            cluster.stop_workers(a)
            start = time()
            while len(client.ncores()) != 1:
                sleep(0.2)
                assert time() < start + 60
    assert not os.path.exists(local_dir)


def test_stop_workers_politely(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        with Client(cluster, loop=loop) as client:
            cluster.start_workers(2)

            while len(client.ncores()) < 2:
                sleep(0.1)

            futures = client.scatter(list(range(10)))

            a, b = cluster.workers
            cluster.stop_workers(a)

            data = client.gather(futures)
            assert data == list(range(10))


def test_logs(loop):
    with DRMAACluster(scheduler_port=0) as cluster:
        cluster.start_workers(2)
        while len(cluster.scheduler.workers) < 2:
            sleep(0.1)

        for w in cluster.workers:
            fn = 'worker.%s.err' % w
            assert os.path.exists(fn)
            with open(fn) as f:
                assert "worker" in f.read()


def test_stdout_in_worker():
    """
    stdout and stderr should be redirected and line-buffered in workers.
    """
    def inc_and_print(x):
        print("stdout: inc_and_print(%s)" % (x,))
        print("stderr: inc_and_print(%s)" % (x,), file=sys.stderr)
        return x + 1

    def get_lines(fn):
        with open(fn) as f:
            return [line.strip() for line in f]

    with DRMAACluster(scheduler_port=0, diagnostics_port=None) as cluster:
        with Client(cluster) as client:
            cluster.start_workers(1)
            future = client.submit(inc_and_print, 1)
            assert future.result() == 2

            w, = cluster.workers.values()
            assert "stdout: inc_and_print(1)" in get_lines(w.stdout)
            assert "stderr: inc_and_print(1)" in get_lines(w.stderr)


def test_cleanup():
    """
    Not a test, just ensure that all worker logs are cleaned up at the
    end of the test run.
    """
    def cleanup_logs():
        from glob import glob
        import os
        for fn in glob('worker.*.out'):
            os.remove(fn)
        for fn in glob('worker.*.err'):
            os.remove(fn)

    import atexit
    atexit.register(cleanup_logs)


def test_passed_script(loop):
    with tmpfile(extension='sh') as fn:
        with open(fn, 'w') as f:
            f.write(make_job_script(executable=worker_bin_path,
                                    name='foo'))
        os.chmod(fn, 0o777)
        with DRMAACluster(scheduler_port=0, script=fn) as cluster:
            tmp_script_location = cluster.script
            assert cluster.script.split(os.path.sep)[-1] == fn.split(os.path.sep)[-1]
            job = cluster.start_workers(1)
            with Client(cluster, loop=loop) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11
        assert os.path.exists(fn)  # doesn't cleanup provided script
        assert not os.path.exists(tmp_script_location)
