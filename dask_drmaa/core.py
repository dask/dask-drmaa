import logging
import os
import socket
import sys

import drmaa
from distributed import LocalCluster
from distributed.utils import log_errors


logger = logging.getLogger(__name__)


class DRMAACluster(object):
    def __init__(self,
                 jobName='dask-worker',
                 remoteCommand=os.path.join(sys.exec_prefix, 'bin', 'dask-worker'),
                 args=(),
                 outputPath=':%s/out' % os.getcwd(),
                 errorPath=':%s/err' % os.getcwd(),
                 workingDirectory = os.getcwd(),
                 nativeSpecification='',
                 **kwargs):

        logger.info("Start local scheduler")
        self.local_cluster = LocalCluster(n_workers=0, **kwargs)
        self.session = drmaa.Session()
        self.session.initialize()
        logger.info("Initialize connection to job scheduler")

        self.jobName = jobName
        self.remoteCommand = remoteCommand
        self.args = ['%s:%d' % (socket.gethostname(),
                     self.local_cluster.scheduler.port)] + list(args)
        self.outputPath = outputPath
        self.errorPath = errorPath
        self.nativeSpecification = nativeSpecification

        self.workers = set()

    @property
    def scheduler(self):
        return self.local_cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def createJobTemplate(self, nativeSpecification=''):
        wt = self.session.createJobTemplate()
        wt.jobName = self.jobName
        wt.remoteCommand = self.remoteCommand
        wt.args = self.args
        wt.outputPath = self.outputPath
        wt.errorPath = self.errorPath
        wt.nativeSpecification = self.nativeSpecification + ' ' + nativeSpecification
        return wt

    def start_workers(self, n=1, **kwargs):
        with log_errors():
            wt = self.createJobTemplate(**kwargs)

            ids = self.session.runBulkJobs(wt, 1, n, 1)
            logger.info("Start %d workers. Job ID: %s", len(ids), ids[0].split('.')[0])
            self.workers.update(ids)

    def stop_workers(self, worker_ids, sync=False):
        worker_ids = list(worker_ids)
        for wid in worker_ids:
            try:
                self.session.control(wid, drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass
            self.workers.remove(wid)

        logger.info("Stop workers %s", worker_ids)
        if sync:
            self.session.synchronize(worker_ids, dispose=True)

    def close(self):
        self.local_cluster.close()
        if self.workers:
            self.stop_workers(self.workers, sync=True)
        try:
            self.session.exit()
        except drmaa.errors.NoActiveSessionException:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass

    def __str__(self):
        return "<%s: %d workers>" % (self.__class__.__name__, len(self.workers))

    __repr__ = __str__


class SGECluster(DRMAACluster):
    def createJobTemplate(self, nativeSpecification='', cpus=1, memory=None):
        args = self.args
        ns = self.nativeSpecification
        if nativeSpecification:
            ns = ns + nativeSpecification
        if memory:
            args = args + ['--memory-limit', str(memory * 0.6)]
            args = args + ['--resources', 'memory=%f' % (memory * 0.8)]
            ns += ' -l h_vmem=%dG' % int(memory / 1e9) # / cpus
        if cpus:
            args = args + ['--nprocs', '1', '--nthreads', str(cpus)]
            # ns += ' -l TODO=%d' % (cpu + 1)
        args = args + ['--name', '$PBS_JOBID']

        wt = self.session.createJobTemplate()
        wt.jobName = self.jobName
        wt.remoteCommand = self.remoteCommand
        wt.args = args
        wt.outputPath = self.outputPath
        wt.errorPath = self.errorPath
        wt.nativeSpecification = ns

        return wt
