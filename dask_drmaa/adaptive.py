from __future__ import print_function, division, absolute_import

import logging
import warnings

from distributed import Scheduler
from distributed.utils import log_errors
from distributed.deploy import adaptive
from tornado import gen

from .core import get_session

logger = logging.getLogger(__file__)


class Adaptive(adaptive.Adaptive):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.

    Parameters
    ----------
    cluster: object
        Must have scale_up and scale_down methods/coroutines
    scheduler: distributed.Scheduler

    Examples
    --------
    >>> class MyCluster(object):
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove worker addresses from cluster """
    '''
    def __init__(self, cluster=None, scheduler=None, interval=1000,
                 startup_cost=1, scale_factor=2, **kwargs):
        if cluster is None:
            raise TypeError("`Adaptive.__init__() missing required argument: "
                            "`cluster`")

        if isinstance(cluster, Scheduler):
            warnings.warn("The ``cluster`` and ``scheduler`` arguments to "
                          "Adaptive.__init__ will switch positions in a future"
                          " release. Please use keyword arguments.",
                          FutureWarning)
            cluster, scheduler = scheduler, cluster
        if scheduler is None:
            scheduler = cluster.scheduler

        super(Adaptive, self).__init__(scheduler, cluster, interval,
                                       startup_cost=startup_cost,
                                       scale_factor=scale_factor,
                                       **kwargs)

    def get_busy_workers(self):
        s = self.scheduler
        busy = {w for w in s.workers
                if len(s.processing[w]) > 2 * s.ncores[w]
                and s.occupancy[w] > self.startup_cost * 2}
        return busy

    def needs_cpu(self):
        # don't want to call super(), since it ignores number of tasks
        s = self.scheduler
        busy = self.get_busy_workers()
        if s.unrunnable or busy:
            if any(get_session().jobStatus(jid) == 'queued_active' for
                    jid in self.cluster.workers):  # TODO: is this slow?
                return False
            if len(s.workers) < len(self.cluster.workers):
                # TODO: this depends on reliable cleanup of closed workers
                return False
            return True

    def get_scale_up_kwargs(self):
        instances = max(1, len(self.scheduler.ncores) * self.scale_factor)
        kwargs = {'n': max(instances, len(self.get_busy_workers()))}
        memory = []
        if self.scheduler.unrunnable:
            for task in self.scheduler.unrunnable:
                key = task.key
                prefix = task.prefix
                duration = 0
                memory = []
                duration += self.scheduler.task_duration.get(prefix, 0.1)

                if key in self.scheduler.resource_restrictions:
                    m = self.scheduler.resource_restrictions[key].get('memory')
                    if m:
                        memory.append(m)
        if memory:
            kwargs['memory'] = max(memory) * 4
        logger.info("Starting workers due to resource constraints: %s",
                    kwargs['n'])
        return kwargs

    @gen.coroutine
    def _retire_workers(self, workers=None):
        if workers is None:
            workers = self.workers_to_close()
        if not workers:
            raise gen.Return(workers)
        with log_errors():
            result = yield self.scheduler.retire_workers(workers,
                                                         remove=True,
                                                         close_workers=True)
            if result:
                logger.info("Retiring workers {}".format(result))
            # Diverges from distributed.Adaptive here:
            # ref c51a15a35a8a64c21c1182bfd9209cb6b7d95380
            # TODO: can this be reconciled back to base class implementation?
        raise gen.Return(result)
