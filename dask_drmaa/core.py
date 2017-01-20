import logging
import os
import socket
import sys

import drmaa
from toolz import merge
from tornado.ioloop import PeriodicCallback

from distributed import LocalCluster
from distributed.utils import log_errors, ignoring

logger = logging.getLogger(__name__)


_global_session = [None]

def get_session():
    if not _global_session[0]:
        _global_session[0] = drmaa.Session()
        _global_session[0].initialize()
    return _global_session[0]


default_template = {
    'remoteCommand': os.path.join(sys.exec_prefix, 'bin', 'dask-worker'),
    'jobName': 'dask-worker',
    'outputPath': ':%s/out' % os.getcwd(),
    'errorPath': ':%s/err' % os.getcwd(),
    'workingDirectory': os.getcwd(),
    'nativeSpecification': '',
    'args': []
    }


class DRMAACluster(object):
    def __init__(self, template=None, cleanup_interval=1000, hostname=None, **kwargs):
        """
        Dask workers launched by a DRMAA-compatible cluster

        Parameters
        ----------
        jobName: string
            Name of the job as known by the DRMAA cluster.
        remoteCommand: string
            Path to the dask-worker executable
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
        self.local_cluster = LocalCluster(n_workers=0, **kwargs)

        self.template = merge(default_template, template or {})

        self._cleanup_callback = PeriodicCallback(callback=self.cleanup_closed_workers,
                                                  callback_time=cleanup_interval,
                                                  io_loop=self.scheduler.loop)
        self._cleanup_callback.start()

        self.workers = {}  # {job-id: {'resource': quanitty}}

    @property
    def scheduler(self):
        return self.local_cluster.scheduler

    @property
    def scheduler_address(self):
        return '%s:%d' % (self.hostname, self.scheduler.port)

    def createJobTemplate(self, **kwargs):
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
            with self.createJobTemplate(**kwargs) as jt:
                ids = get_session().runBulkJobs(jt, 1, n, 1)
                logger.info("Start %d workers. Job ID: %s", len(ids), ids[0].split('.')[0])
                self.workers.update({jid: kwargs for jid in ids})
                global_workers.update(ids)

    def stop_workers(self, worker_ids, sync=False):
        worker_ids = list(worker_ids)
        for wid in worker_ids:
            try:
                get_session().control(wid, drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass
            self.workers.pop(wid)

            with ignoring(KeyError):
                global_workers.remove(wid)

        logger.info("Stop workers %s", worker_ids)
        if sync:
            get_session().synchronize(worker_ids, dispose=True)

    def close(self):
        logger.info("Closing DRMAA cluster")
        self.local_cluster.close()
        if self.workers:
            self.stop_workers(self.workers, sync=True)

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


global_workers = set()


def remove_workers():
    if not get_session():
        return

    for wid in global_workers:
        try:
            get_session().control(wid, drmaa.JobControlAction.TERMINATE)
        except drmaa.errors.InvalidJobException:
            pass


import atexit
atexit.register(remove_workers)
