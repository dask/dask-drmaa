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
                 max_runtime='1:00:00', #1 hour
                 **kwargs):
        """
        Parameters
        ----------
        jobName: string name of the job as known by the DRMAA cluster.
                 For an SGE cluster, this can be seen with the "qstat" command.
        remoteCommand: The path to the executable that this cluster should execute in a job by default
        args: an iterable of arguments for the remoteCommand application
        outputPath: string for the path where stdout data should be stored
        errorPath: string for the path where stderr data should be stored
        workingDirector: string for the working path where the remoteCommand should execute
        nativeSpecification: string of options native to the specific cluster/batch scheduler that the job will run on
        max_runtime: string of "hours:minutes:seconds" telling the default maximum runtime of a job
        """
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
        self.max_runtime = max_runtime

        self.workers = {}  # {job-id: {'resource': quanitty}}

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
            self.workers.update({jid: kwargs for jid in ids})

    def stop_workers(self, worker_ids, sync=False):
        worker_ids = list(worker_ids)
        for wid in worker_ids:
            try:
                self.session.control(wid, drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass
            self.workers.pop(wid)

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

    def jobStatus(self, jid):
        """Return the DRMAA job status object.
        This hold stuff like start time, wall clock limit, job requirements, etc. as attributes
        This is a simple wrapper for the DRMAA library, and matches its name
        """
        status = self.session.jobStatus(jid)
        return status

    def startTime(self, jid):
        """Return a timestamp string of the job start time.
        This is a simple wrapper for the DRMAA library, and matches its name
        """
        status = self.jobStatus(jid)
        return status.startTime
        
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
    default_memory = None
    default_memory_fraction = 0.6
    def createJobTemplate(self, nativeSpecification='', cpus=1, memory=None,
            memory_fraction=None):
        memory = memory or self.default_memory
        memory_fraction = memory_fraction or self.default_memory_fraction

        args = self.args
        ns = self.nativeSpecification
        if nativeSpecification:
            ns = ns + nativeSpecification
        if memory:
            args = args + ['--memory-limit', str(memory * memory_fraction)]
            args = args + ['--resources', 'memory=%f' % (memory * 0.8)]
            ns += ' -l h_vmem=%dG' % int(memory / 1e9) # / cpus
        if cpus:
            args = args + ['--nprocs', '1', '--nthreads', str(cpus)]
            # ns += ' -l TODO=%d' % (cpu + 1)

        ns += ' -l h_rt={}'.format(self.max_runtime)
        
        wt = self.session.createJobTemplate()
        wt.jobName = self.jobName
        wt.remoteCommand = self.remoteCommand
        wt.args = args
        wt.outputPath = self.outputPath
        wt.errorPath = self.errorPath
        wt.nativeSpecification = ns

        return wt
