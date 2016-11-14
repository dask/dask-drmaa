Dask on DRMAA
=============

|Build Status|

Deploy a Dask.distributed_ cluster on top of a cluster running a
DRMAA_-compliant job scheduler.


Example
-------

Launch from Python

.. code-block:: python

   from dask_drmaa import DRMAACluster
   cluster = DRMAACluster()

   from dask.distributed import Client
   client = Client(cluster)
   client.start_workers(2)

   >>> future = client.submit(lambda x: x + 1, 10)
   >>> future.result()
   11

Or launch from the command line::

    $ dask-drmaa 10  # starts local scheduler and ten remote workers


Install
-------

Currently this is only available through GitHub and source installation::

    pip install git+https://github.com/dask/dask-drmaa.git --upgrade

or::

    git clone git@github.com:dask/dask-drmaa.git
    cd dask-drmaa
    python setup.py install

You must have the DRMAA system library installed and be able to submit jobs
from your local machine.


Testing
-------

This repository contains a Docker-compose testing harness for a Son of Grid
Engine cluster with a master and two slaves.   You can initialize this system
as follows

.. code-block:: bash

   docker-compose build
   ./start-sge.sh

And run tests with py.test in the master docker container

.. code-block:: bash

  docker exec -it sge_master /bin/bash -c "cd /dask-drmaa; python setup.py develop"
  docker exec -it sge_master py.test dask-drmaa/dask_drmaa --verbose


Related Work
------------

*  DRMAA_: The Distributed Resource Management Application API, a high level
  API for general use on traditional job schedulers
*  drmaa-python_: The Python bindings for DRMAA
*  DaskSGE_: An earlier dask-drmaa implementation
*  `Son of Grid Engine`_: The default implementation used in testing
*  Dask.distributed_: The actual distributed computing library this launches

.. _DRMAA: https://www.drmaa.org/
.. _drmaa-python: http://drmaa-python.readthedocs.io/en/latest/
.. _`Son of Grid Engine`: https://arc.liv.ac.uk/trac/SGE
.. _dasksge: https://github.com/mfouesneau/dasksge
.. _Dask.distributed: http://distributed.readthedocs.io/en/latest/
.. _DRMAA: https://www.drmaa.org/


.. |Build Status| image:: https://travis-ci.org/dask/dask-drmaa.svg?branch=master
   :target: https://travis-ci.org/dask/dask-drmaa
