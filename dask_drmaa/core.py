from collections import namedtuple
import logging
import os
import shutil
import socket
import sys
import tempfile

import drmaa
from toolz import merge
from tornado import gen

from distributed import LocalCluster
from distributed.deploy import Cluster
from distributed.utils import log_errors, ignoring
from distributed.utils import PeriodicCallback

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
_session = drmaa.Session()
_drm_info = _session.drmsInfo
_drmaa_implementation = _session.drmaaImplementation

if "SLURM" in _drm_info:
    JOB_PARAM = "%j"
    JOB_ID = "$SLURM_JOB_ID"
    TASK_ID = "$SLURM_ARRAY_TASK_ID"
elif "LSF" in _drm_info:
    JOB_PARAM = "%J"
    JOB_ID = "$LSB_JOBID"
    TASK_ID = "$LSB_JOBINDEX"
elif "GE" in _drm_info:
    JOB_PARAM = "$JOB_ID"
    JOB_ID = "$JOB_ID"
    TASK_ID = "$SGE_TASK_ID"
elif "Torque" == _drm_info or "PBS" in _drmaa_implementation:
    JOB_PARAM = "$PBS_JOBID"
    JOB_ID = "$PBS_JOBID"
    TASK_ID = "$PBS_TASKNUM"
else:
    JOB_PARAM = ""
    JOB_ID = ""
    TASK_ID = ""

worker_out_path_template = os.path.join(
    os.getcwd(),
    'worker.%(jid)s.%(ext)s'
)

default_template = {
    'jobName': 'dask-worker',
    'outputPath': ':' + worker_out_path_template % dict(
        jid=".".join([JOB_PARAM, '$drmaa_incr_ph$']), ext='out'
    ),
    'errorPath': ':' + worker_out_path_template % dict(
        jid=".".join([JOB_PARAM, '$drmaa_incr_ph$']), ext='err'
    ),
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


class DRMAACluster(Cluster):
    def __init__(self, template=None, cleanup_interval=1000, hostname=None,
                 script=None, preexec_commands=(), copy_script=True,
                 ip='',
                 **kwargs):
        """
        Dask workers launched by a DRMAA-compatible cluster

        Parameters
        ----------
        template: dict
            Dictionary specifying options to pass to the DRMAA cluster
            and the worker. Relevant items are:

            jobName: string
                Name of the job as known by the DRMAA cluster.
            args: list
                Extra string arguments to pass to dask-worker
            outputPath: string
                Path to the dask-worker stdout. Must start with ':'.
                Defaults to worker.JOBID.TASKID.out in current directory.
            errorPath: string
                Path to the dask-worker stderr. Must start with ':'
                Defaults to worker.JOBID.TASKID.err in current directory.
            workingDirectory: string
                Where dask-worker runs, defaults to current directory
            nativeSpecification: string
                Options native to the job scheduler

        cleanup_interval: int
            Time interval in seconds at which closed workers are cleaned.
            Defaults to 1000
        hostname: string
            Host on which to start the local scheduler, defaults to localhost
        script: string (optional)
            Path to the dask-worker executable script.
            A temporary file will be made if none is provided (recommended)
        preexec_commands: tuple (optional)
            Commands to be executed first by temporary script. Cannot be
            specified at the same time as script.
        copy_script: bool
            Whether should copy the passed script to the current working
            directory. This is primarily to work around an issue with SGE.
        ip: string
            IP of the scheduler, default is the empty string
            which will listen on the primary ip address of the host
        **kwargs:
            Additional keyword arguments to be passed to the local scheduler

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
        self.local_cluster = LocalCluster(n_workers=0, ip=ip, **kwargs)

        if script is None:
            fn = tempfile.mktemp(suffix='sh',
                                 prefix='dask-worker-script',
                                 dir=os.path.curdir)
            self.script = fn
            self._should_cleanup_script = True

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
            self._should_cleanup_script = False
            if copy_script:
                with ignoring(EnvironmentError):  # may be in the same path
                    shutil.copy(script, os.path.curdir)  # python 2.x returns None
                    script = os.path.join(os.path.curdir, os.path.basename(script))
                    self._should_cleanup_script = True
            self.script = script
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


    def adapt(self, **kwargs):
        """ Turn on adaptivity

        For keyword arguments see dask_drmaa.adaptive.Adaptive

        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum=10, interval='500ms')

        See Also
        --------
        Cluster: an interface for other clusters to inherit from
        """
        from .adaptive import Adaptive

        with ignoring(AttributeError):
            self._adaptive.stop()
        if not hasattr(self, '_adaptive_options'):
            self._adaptive_options = {}

        self._adaptive_options.update(kwargs)
        self._adaptive = Adaptive(
            self, self.scheduler, **self._adaptive_options
        )

        return self._adaptive

    @gen.coroutine
    def _start(self):
        pass

    @property
    def scheduler(self):
        return self.local_cluster.scheduler

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
        if n == 0:
            return

        with log_errors():
            with self.create_job_template(**kwargs) as jt:
                ids = get_session().runBulkJobs(jt, 1, n, 1)
                logger.info("Start %d workers. Job ID: %s", len(ids), ids[0].split('.')[0])
                self.workers.update(
                    {jid: WorkerSpec(job_id=jid, kwargs=kwargs,
                                     stdout=worker_out_path_template % dict(jid=jid, ext='out'),
                                     stderr=worker_out_path_template % dict(jid=jid, ext='err'),
                                     )
                     for jid in ids})

    @gen.coroutine
    def stop_workers(self, worker_ids, sync=False):
        if isinstance(worker_ids, str):
            worker_ids = [worker_ids]
        elif worker_ids:
            worker_ids = list(worker_ids)
        else:
            return

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
        self.stop_workers(self.workers, sync=True)

        self.local_cluster.close()
        if self._should_cleanup_script and os.path.exists(self.script):
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
