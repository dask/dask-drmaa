from .core import DRMAACluster, get_session


class SGECluster(DRMAACluster):
    default_memory = None

    def start_workers(self, n=1, nativeSpecification='', cpus=1, memory=None,
                      memory_fraction=0.5):
        '''
        Start dask workers on an SGE cluster.

        Parameters
        ----------
        n: integer
            Number of dask workers to start
        nativeSpecification: string (optional)
            Specify options native to the job scheduler
        cpus: integer
            Number of threads per dask worker
        memory: integer (optional)
            Number of bytes of RAM per dask worker
        memory_fraction: float
            If memory is specified, specify the fraction of memory used
            before dask workers begin to cache data to disk
        '''
        return super(SGECluster, self).start_workers(
            n=n, nativeSpecification=nativeSpecification, cpus=cpus,
            memory=memory, memory_fraction=memory_fraction)

    def create_job_template(self, nativeSpecification='', cpus=1, memory=None,
            memory_fraction=0.5):
        memory = memory or self.default_memory
        template = self.template.copy()

        ns = template['nativeSpecification']
        args = template['args']

        args = [self.scheduler_address] + template['args']

        if nativeSpecification:
            ns = ns + nativeSpecification
        if memory:
            args = args + ['--memory-limit', str(memory * (1 - memory_fraction))]
            args = args + ['--resources', 'memory=%f' % (memory * memory_fraction)]
            ns += ' -l h_vmem=%dG' % int(memory / 1e9) # / cpus
        if cpus:
            args = args + ['--nprocs', '1', '--nthreads', str(cpus)]
            # ns += ' -l TODO=%d' % (cpu + 1)

        template['nativeSpecification'] = ns
        template['args'] = args

        jt = get_session().createJobTemplate()
        valid_attributes = dir(jt)

        for key, value in template.items():
            if key not in valid_attributes:
                raise ValueError("Invalid job template attribute %s" % key)
            setattr(jt, key, value)

        return jt
