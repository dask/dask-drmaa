import os
import socket
import sys

import drmaa
from distributed import LocalCluster

class DRMAACluster(object):
    def __init__(self, **kwargs):
        self.local_cluster = LocalCluster(n_workers=0, **kwargs)
        self.session = drmaa.Session()
        self.session.initialize()

        self.worker_template = self.session.createJobTemplate()
        self.worker_template.remoteCommand = os.path.join(sys.exec_prefix, 'bin', 'dask-worker')
        self.worker_template.jobName = 'dask-worker'
        self.worker_template.args = ['%s:%d' % (socket.gethostname(), self.local_cluster.scheduler.port)]
        self.worker_template.outputPath = ':/%s/out' % os.getcwd()
        self.worker_template.errorPath = ':/%s/err' % os.getcwd()
        self.worker_template.workingDirectory = os.getcwd()

        self.workers = []

    @property
    def scheduler_address(self):
        return self.local_cluster.scheduler_address

    def start_workers(self, n=1):
        ids = self.session.runBulkJobs(self.worker_template, 1, n, 1)
        self.workers.extend(ids)

    def stop_workers(self, worker_ids, sync=False):
        for wid in worker_ids:
            try:
                self.session.control(wid, drmaa.JobControlAction.TERMINATE)
            except drmaa.errors.InvalidJobException:
                pass

        if sync:
            self.session.synchronize(worker_ids, dispose=True)

    def close(self):
        if self.workers:
            self.stop_workers(self.workers, sync=True)
        self.local_cluster.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass
