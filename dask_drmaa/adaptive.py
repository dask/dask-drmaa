from __future__ import print_function, division, absolute_import

import logging
from distributed.utils import log_errors

from toolz import first
from tornado import gen
from tornado.ioloop import PeriodicCallback

from .core import get_session

logger = logging.getLogger(__file__)


class Adaptive(object):
    '''
    Adaptively allocate workers based on scheduler load.  A superclass.

    Contains logic to dynamically resize a Dask cluster based on current use.

    Parameters
    ----------
    scheduler: distributed.Scheduler
    cluster: object
        Must have scale_up and scale_down methods/coroutines

    Examples
    --------
    >>> class MyCluster(object):
    ...     def scale_up(self, n):
    ...         """ Bring worker count up to n """
    ...     def scale_down(self, workers):
    ...        """ Remove worker addresses from cluster """
    '''
    def __init__(self, scheduler=None, cluster=None, interval=1000, startup_cost=1):
        self.cluster = cluster
        if scheduler is None:
            scheduler = cluster.scheduler
        self.scheduler = scheduler
        self.startup_cost = startup_cost
        self._adapt_callback = PeriodicCallback(callback=self._adapt,
                                                callback_time=interval,
                                                io_loop=self.scheduler.loop)
        self.scheduler.loop.add_callback(self._adapt_callback.start)
        self._adapting = False

    @gen.coroutine
    def _retire_workers(self):
        """
        Get the cluster scheduler to cleanup any workers it decides can retire
        """
        with log_errors():
            workers = yield self.scheduler.retire_workers(remove=True, close=True)
            logger.info("Retiring workers {}".format(workers))

    @gen.coroutine
    def _adapt(self):
        logger.info("Adapting")
        with log_errors():
            if self._adapting:  # Semaphore to avoid overlapping adapt calls
                return

            s = self.scheduler

            self._adapting = True
            try:
                if s.unrunnable:
                    duration = 0
                    memory = []
                    for key in s.unrunnable:
                        duration += s.task_duration.get(key, 0.1)
                        if key in s.resource_restrictions:
                            m = s.resource_restrictions[key].get('memory')
                            if m:
                                memory.append(m)

                    # Here we should be clever about choosing the right suite
                    # of workers to request.  Instead we just request one with
                    # memory to cover the largest task.  But only if there are
                    # no other reequests in flight.

                    if any(get_session().jobStatus(jid) == 'queued_active' for
                            jid in self.cluster.workers):  # TODO: is this slow?
                        return

                    logger.info("Starting worker")
                    if memory:
                        self.cluster.start_workers(1, memory=max(memory) * 2)
                    else:
                        self.cluster.start_workers(1)

                yield self._retire_workers()
            finally:
                self._adapting = False

    def adapt(self):
        self.scheduler.loop.add_callback(self._adapt)
