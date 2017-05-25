
import logging
import signal
import sys
from time import sleep

import click

from dask_drmaa import DRMAACluster
from distributed.cli.utils import check_python_3


@click.command()
@click.argument('nworkers', type=int)
def main(nworkers):
    cluster = DRMAACluster(silence_logs=logging.INFO, scheduler_port=8786)
    cluster.start_workers(nworkers)

    def handle_signal(sig, frame):
        cluster.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        sleep(1)


def go():
    check_python_3()
    main()

if __name__ == '__main__':
    go()
