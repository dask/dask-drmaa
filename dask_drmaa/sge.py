from .core import DRMAACluster, get_session


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

        wt = get_session().createJobTemplate()
        wt.jobName = self.jobName
        wt.remoteCommand = self.remoteCommand
        wt.args = args
        wt.outputPath = self.outputPath
        wt.errorPath = self.errorPath
        wt.nativeSpecification = ns

        return wt
