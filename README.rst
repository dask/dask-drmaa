Dask on DRMAA
=============

|Build Status| |PyPI Release| |conda-forge Release|

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
   cluster.start_workers(2)

   >>> future = client.submit(lambda x: x + 1, 10)
   >>> future.result()
   11

Or launch from the command line::

    $ dask-drmaa 10  # starts local scheduler and ten remote workers


Install
-------

Python packages are available from PyPI and can be installed with ``pip``::

    pip install dask-drmaa

Also ``conda`` packages are available from conda-forge::

    conda install -c conda-forge dask-drmaa

Additionally the package can be installed from GitHub with the latest changes::

    pip install git+https://github.com/dask/dask-drmaa.git --upgrade

or::

    git clone git@github.com:dask/dask-drmaa.git
    cd dask-drmaa
    pip install .

You must have the DRMAA system library installed and be able to submit jobs
from your local machine. Please make sure to set the environment variable
``DRMAA_LIBRARY_PATH`` to point to the location of ``libdrmaa.so`` for your
system.


Testing
-------

This repository contains a Docker-compose testing harness for a Son of Grid
Engine cluster with a master and two slaves.  You can initialize this system
as follows:

.. code-block:: bash

   docker-compose build
   ./start-sge.sh

If you have done this previously and need to refresh your solution you can do
the following

.. code-block:: bash

   docker-compose stop
   docker-compose build --no-cache
   ./start-sge.sh

And run tests with py.test in the master docker container

.. code-block:: bash

  docker exec -it sge_master /bin/bash -c "cd /dask-drmaa; python setup.py develop"
  docker exec -it sge_master /bin/bash -c "cd /dask-drmaa; py.test dask_drmaa --verbose"


Adaptive Load
-------------

Dask-drmaa can adapt to scheduler load, deploying more workers on the grid when
it has more work, and cleaning up these workers when they are no longer
necessary.  This can simplify setup (you can just leave a cluster running) and
it can reduce load on the cluster, making IT happy.

To enable this, call the ``Adaptive`` class on a ``DRMAACluster``.  You can
submit computations to the cluster without ever explicitly creating workers.

.. code-block:: python

   from dask_drmaa import DRMAACluster, Adaptive
   from dask.distributed import Client

   cluster = DRMAACluster()
   adapt = Adaptive(cluster)
   client = Client(cluster)

   futures = client.map(func, seq)  # workers will be created as necessary


Extensible
----------

The DRMAA interface is the lowest common denominator among many different job
schedulers like SGE, SLURM, LSF, Torque, and others.  However, sometimes users
need to specify parameters particular to their cluster, such as resource
queues, wall times, memory constraints, etc..

DRMAA allows users to pass native specifications either when constructing the
cluster or when starting new workers:

.. code-block:: python

   cluster = DRMAACluster(template={'nativeSpecification': '-l h_rt=01:00:00'})
   # or
   cluster.start_workers(10, nativeSpecification='-l h_rt=01:00:00')


Related Work
------------

* DRMAA_: The Distributed Resource Management Application API, a high level
  API for general use on traditional job schedulers
* drmaa-python_: The Python bindings for DRMAA
* DaskSGE_: An earlier dask-drmaa implementation
* `Son of Grid Engine`_: The default implementation used in testing
* Dask.distributed_: The actual distributed computing library this launches

.. _DRMAA: https://www.drmaa.org/
.. _drmaa-python: http://drmaa-python.readthedocs.io/en/latest/
.. _`Son of Grid Engine`: https://arc.liv.ac.uk/trac/SGE
.. _dasksge: https://github.com/mfouesneau/dasksge
.. _Dask.distributed: http://distributed.readthedocs.io/en/latest/
.. _DRMAA: https://www.drmaa.org/


.. |Build Status| image:: https://travis-ci.org/dask/dask-drmaa.svg?branch=master
   :target: https://travis-ci.org/dask/dask-drmaa

.. |PyPI Release| image:: https://img.shields.io/pypi/v/dask-drmaa.svg
   :target: https://pypi.python.org/pypi/dask-drmaa

.. |conda-forge Release| image:: https://img.shields.io/conda/vn/conda-forge/dask-drmaa.svg
   :target: https://github.com/conda-forge/dask-drmaa-feedstock
