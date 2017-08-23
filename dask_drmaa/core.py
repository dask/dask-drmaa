from collections import namedtuple
import logging
import os
import socket
import sys
import tempfile

import drmaa
from toolz import merge
from tornado import gen
from tornado.ioloop import PeriodicCallback

from distributed import LocalCluster
from distributed.utils import log_errors, ignoring
from distributed.protocol.pickle import dumps

logger = logging.getLogger(__name__)


_global_session = [None]

def get_session():
    if not _global_session[0]:
        _global_session[0] = drmaa.Session()
        _global_session[0].initialize()
    return _global_session[0]


WorkerSpec = namedtuple('WorkerSpec',
                        ('job_id', 'kwargs', 'stdout', 'stderr'))


worker_bin_path = os.path.join(sys.exec_prefix, 'bin', 'dask-worker')

# All JOB_ID and TASK_ID environment variables
JOB_ID = "$JOB_ID$SLURM_JOB_ID$LSB_JOBID"
TASK_ID = "$SGE_TASK_ID$SLURM_ARRAY_TASK_ID$LSB_JOBINDEX"

worker_out_path_template = os.path.join(os.getcwd(), 'worker.%(jid)s.%(kind)s')

default_template = {
    'jobName': 'dask-worker',
    'outputPath': ':' + worker_out_path_template % dict(jid='$JOB_ID.$drmaa_incr_ph$', kind='out'),
    'errorPath': ':' + worker_out_path_template % dict(jid='$JOB_ID.$drmaa_incr_ph$', kind='err'),
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

        self.workers = {}  # {job-id: WorkerSpec}

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

    def start_workers(self, n=1, **kwargs):
        with log_errors():
            with self.create_job_template(**kwargs) as jt:
                ids = get_session().runBulkJobs(jt, 1, n, 1)
                logger.info("Start %d workers. Job ID: %s", len(ids), ids[0].split('.')[0])
                self.workers.update(
                    {jid: WorkerSpec(job_id=jid, kwargs=kwargs,
                                     stdout=worker_out_path_template % dict(jid=jid, kind='out'),
                                     stderr=worker_out_path_template % dict(jid=jid, kind='err'),
                                     )
                     for jid in ids})

    @gen.coroutine
    def stop_workers(self, worker_ids, sync=False):
        if isinstance(worker_ids, str):
            worker_ids = [worker_ids]
        else:
            worker_ids = list(worker_ids)

        # Let the scheduler gracefully retire workers first
        ids_to_ips = {
            v['name']: k for k, v in self.scheduler.worker_info.items()
        }
        worker_ips = [ids_to_ips[wid]
                      for wid in worker_ids
                      if wid in ids_to_ips]
        retired = yield self.scheduler.retire_workers(workers=worker_ips,
                                                      close_workers=True)
        logger.info("Retired workers %s", retired)
        for wid in list(worker_ids):
            try:
                get_session().control(wid, drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass
            try:
                self.workers.pop(wid)
            except KeyError:
                # If we have multiple callers at once, it may have already
                # been popped off
                pass

        logger.info("Stop workers %s", worker_ids)
        if sync:
            get_session().synchronize(worker_ids, dispose=True)

    @gen.coroutine
    def scale_up(self, n, **kwargs):
        yield [self.start_workers(**kwargs)
               for _ in range(n - len(self.workers))]

    @gen.coroutine
    def scale_down(self, workers):
        workers = set(workers)
        yield self.scheduler.retire_workers(workers=workers)

    def close(self):
        logger.info("Closing DRMAA cluster")
        if self.workers:
            self.stop_workers(self.workers, sync=True)

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
