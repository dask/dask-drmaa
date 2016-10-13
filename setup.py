#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='dask-drmaa',
      version='0.1.0',
      description='Dask on DRMAA',
      url='http://github.com/dask/dask-drmaa/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='',
      packages=['dask_drmaa',
                'dask_drmaa.cli'],
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      entry_points='''
      [console_scripts]
      dask-drmaa=dask_drmaa.cli.dask_drmaa:go
      ''',
      zip_safe=False)
