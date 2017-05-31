from __future__ import print_function, division, absolute_import

from collections import namedtuple
import logging
import os
import socket
import sys
import tempfile

import drmaa
from toolz import merge
from tornado import gen
from tornado.queues import Queue, QueueEmpty
from tornado.ioloop import IOLoop, PeriodicCallback

from distributed import LocalCluster, Client
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.utils import log_errors, ignoring, sync

logger = logging.getLogger(__name__)


_global_session = [None]

def get_session():
    if not _global_session[0]:
        _global_session[0] = drmaa.Session()
        _global_session[0].initialize()
    return _global_session[0]


WorkerSpec = namedtuple('WorkerSpec',
                        ('id', 'address', 'kwargs', 'stdout', 'stderr'))


worker_bin_path = os.path.join(sys.exec_prefix, 'bin', 'dask-worker')

# All JOB_ID and TASK_ID environment variables
JOB_IDS = ['JOB_ID', 'SLURM_JOB_ID', 'LSB_JOBID']
JOB_ID = ''.join('$' + s for s in JOB_IDS)
TASK_IDS = ['SGE_TASK_ID', 'SLURM_ARRAY_TASK_ID', 'LSB_JOBINDEX']
TASK_ID = ''.join('$' + s for s in TASK_IDS)

worker_out_path_template = os.path.join(os.getcwd(), 'worker.%(id)s.%(kind)s')

default_template = {
    'jobName': 'dask-worker',
    'outputPath': ':' + worker_out_path_template % dict(id='$JOB_ID.$drmaa_incr_ph$', kind='out'),
    'errorPath': ':' + worker_out_path_template % dict(id='$JOB_ID.$drmaa_incr_ph$', kind='err'),
    'workingDirectory': os.getcwd(),
    'nativeSpecification': '',
    # stdout/stderr are redirected to files, make sure their contents don't lag
    'jobEnvironment': {'PYTHONUNBUFFERED': '1'},
    'args': []
    }


def make_job_script(executable, name, preexec=()):
    shebang = '#!/bin/bash'
    execute = (
        '%(executable)s $1 --name %(name)s "${@:2}"'
        % dict(executable=executable, name=name)
        )
    preparation = list(preexec)
    script_template = '\n'.join([shebang] + preparation + [execute, ''])
    return script_template



class DRMAASchedulerPlugin(SchedulerPlugin):

    def __init__(self, cluster):
        self.cluster = cluster

    def add_worker(self, scheduler, worker):
        self.cluster._worker_updates.put_nowait((worker, 'add'))

    def remove_worker(self, scheduler, worker):
        self.cluster._worker_updates.put_nowait((worker, 'remove'))
        wid = self.cluster.worker_addresses.pop(worker, None)
        if wid:
            del self.cluster.workers[wid]


class DRMAACluster(object):
    def __init__(self, template=None, cleanup_interval=1000, hostname=None,
                 script=None, preexec_commands=(), **kwargs):
        """
        Dask workers launched by a DRMAA-compatible cluster

        Parameters
        ----------
        jobName: string
            Name of the job as known by the DRMAA cluster.
        script: string (optional)
            Path to the dask-worker executable script.
            A temporary file will be made if none is provided (recommended)
        args: list
            Extra string arguments to pass to dask-worker
        outputPath: string
        errorPath: string
        workingDirectory: string
            Where dask-worker runs, defaults to current directory
        nativeSpecification: string
            Options native to the job scheduler

        Examples
        --------
        >>> from dask_drmaa import DRMAACluster          # doctest: +SKIP
        >>> cluster = DRMAACluster()                     # doctest: +SKIP
        >>> cluster.start_workers(10)                    # doctest: +SKIP

        >>> from distributed import Client               # doctest: +SKIP
        >>> client = Client(cluster)                     # doctest: +SKIP

        >>> future = client.submit(lambda x: x + 1, 10)  # doctest: +SKIP
        >>> future.result()                              # doctest: +SKIP
        11
        """
        self.hostname = hostname or socket.gethostname()
        logger.info("Start local scheduler at %s", self.hostname)
        self.local_cluster = LocalCluster(n_workers=0, ip='', **kwargs)
        self.scheduler.add_plugin(DRMAASchedulerPlugin(self))

        if script is None:
            fn = tempfile.mktemp(suffix='sh',
                                 prefix='dask-worker-script',
                                 dir=os.path.curdir)
            self.script = fn

            script_contents = make_job_script(executable=worker_bin_path,
                                              name='%s.%s' % (JOB_ID, TASK_ID),
                                              preexec=preexec_commands)
            with open(fn, 'wt') as f:
                f.write(script_contents)

            @atexit.register
            def remove_script():
                if os.path.exists(fn):
                    os.remove(fn)

            os.chmod(self.script, 0o777)

        else:
            assert not preexec_commands, "Cannot specify both script and preexec_commands"

        # TODO: check that user-provided script is executable

        self.template = merge(default_template,
                              {'remoteCommand': self.script},
                              template or {})

        self._cleanup_callback = PeriodicCallback(callback=self.cleanup_closed_workers,
                                                  callback_time=cleanup_interval,
                                                  io_loop=self.scheduler.loop)
        self._cleanup_callback.start()

        self.workers = {}            # {job-id: WorkerSpec}
        self.worker_addresses = {}   # {address: job-id}
        self._worker_updates = Queue()

    @gen.coroutine
    def _start(self):
        pass

    @property
    def scheduler(self):
        return self.local_cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def create_job_template(self, **kwargs):
        template = self.template.copy()
        if kwargs:
            template.update(kwargs)
        template['args'] = [self.scheduler_address] + template['args']

        jt = get_session().createJobTemplate()
        valid_attributes = dir(jt)

        for key, value in template.items():
            if key not in valid_attributes:
                raise ValueError("Invalid job template attribute %s" % key)
            setattr(jt, key, value)

        return jt

    def start_workers(self, n=1, timeout=20, wait=True, **kwargs):
        """
        Start a number of workers on the cluster.
        If *wait* is true, wait for the workers to register to the
        scheduler and return a list of WorkerSpec instances.
        """
        return sync(self.scheduler.loop, self._start_workers,
                    n=n, timeout=timeout, wait=wait, **kwargs)

    @gen.coroutine
    def _start_workers(self, n=1, timeout=20, wait=True, **kwargs):
        with log_errors():
            with self.create_job_template(**kwargs) as jt:
                ids = get_session().runBulkJobs(jt, 1, n, 1)
                logger.info("Start %d workers. Job ID: %s",
                            len(ids), ids[0].split('.')[0])

            if wait:
                workers = yield self._wait_for_started_workers(
                    ids=ids, timeout=timeout, kwargs=kwargs)
                raise gen.Return(workers)

    @gen.coroutine
    def _wait_for_started_workers(self, ids, timeout, kwargs):
        def get_environ():
            return dict(os.environ)

        deadline = IOLoop.current().time() + timeout

        client = Client(self.local_cluster, start=False)
        yield client._start()

        workers = []
        ids = set(ids)
        n = len(ids)

        while ids:
            n_remaining = len(ids)
            worker_addresses = []
            while len(worker_addresses) < n_remaining:
                try:
                    worker, action = yield self._worker_updates.get(deadline)
                except QueueEmpty:
                    logger.error("Timed out waiting for the following workers: %s",
                                 sorted(ids))
                    yield client._shutdown(fast=True)
                    raise gen.Return(workers)
                if action == 'add':
                    worker_addresses.append(worker)

            # We got enough new workers, see if they correspond
            # to the runBulkJobs request
            environs = yield client._run(get_environ, workers=worker_addresses)

            for w, env in environs.items():
                wid = self._get_worker_id(env, w)
                if wid is None:
                    continue
                if wid not in ids:
                    logger.warning("Got unexpected id %r for worker %r "
                                   "(expected ids: %s)", wid, w,
                                   sorted(ids))
                    continue

                logger.debug("Got worker %r with id %r", w, wid)
                self.workers[wid] = WorkerSpec(
                    id=wid, address=w, kwargs=kwargs,
                    stdout=worker_out_path_template % dict(id=wid, kind='out'),
                    stderr=worker_out_path_template % dict(id=wid, kind='err'),
                    )
                self.worker_addresses[w] = wid
                workers.append(self.workers[wid])
                ids.remove(wid)

        yield client._shutdown(fast=True)
        raise gen.Return(workers)

    def _get_worker_id(self, environ, worker_name):
        """
        Parse a worker's id from its environment variables.
        """
        for envname in JOB_IDS:
            job_id = environ.get(envname, '')
            if job_id:
                break
        else:
            job_id = None
            logger.warning("Could not determine job ID for "
                           "worker %r; available env vars are %s",
                           worker_name, [k for (k, v) in sorted(environ.items()) if v])

        for envname in TASK_IDS:
            task_id = environ.get(envname, '')
            if task_id:
                break
        else:
            task_id = None
            logger.warning("Could not determine task ID for "
                           "worker %r; available env vars are %s",
                           worker_name, [k for (k, v) in sorted(environ.items()) if v])

        if job_id and task_id:
            return '%s.%s' % (job_id, task_id)
        else:
            return None

    def stop_workers(self, worker_ids, wait=False):
        """
        Stop the workers with the given ids.  If *wait* is true, wait
        until their termination is registered by DRMAA.
        """
        worker_ids = sync(self.scheduler.loop, self._stop_workers, worker_ids)
        if wait:
            get_session().synchronize(worker_ids, dispose=True)
        return worker_ids

    @gen.coroutine
    def _stop_workers(self, worker_ids):
        if isinstance(worker_ids, str):
            worker_ids = [worker_ids]
        else:
            worker_ids = list(worker_ids)

        worker_ids = [wid for wid in worker_ids if wid in self.workers]
        workers = [self.workers[wid].address for wid in worker_ids]
        yield self.scheduler.retire_workers(workers=workers, close_workers=True)

        for wid in worker_ids:
            try:
                get_session().control(wid, drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass
            w = self.workers.pop(wid, None)
            if w is not None:
                del self.worker_addresses[w.address]

        logger.info("Stop workers %s", worker_ids)
        raise gen.Return(worker_ids)

    @gen.coroutine
    def _stop_all_workers(self):
        worker_ids = yield self._stop_workers(self.workers)
        raise gen.Return(worker_ids)

    def close(self):
        logger.info("Closing DRMAA cluster")
        worker_ids = sync(self.scheduler.loop, self._stop_all_workers)
        get_session().synchronize(worker_ids, dispose=True)
        self.local_cluster.close()

        if os.path.exists(self.script):
            os.remove(self.script)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def cleanup_closed_workers(self):
        for jid in list(self.workers):
            if get_session().jobStatus(jid) in ('closed', 'done'):
                logger.info("Removing closed worker %s", jid)
                del self.workers[jid]

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __str__(self):
        return "<%s: %d workers>" % (self.__class__.__name__, len(self.workers))

    __repr__ = __str__


def remove_workers():
    get_session().control(drmaa.Session.JOB_IDS_SESSION_ALL,
                          drmaa.JobControlAction.TERMINATE)


import atexit
atexit.register(remove_workers)
