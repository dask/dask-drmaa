[![Build Status](https://travis-ci.org/dask/dask-drmaa.svg?branch=master)](https://travis-ci.org/dask/dask-drmaa)

## Setup

```bash
$ docker-compose build
$ ./start-sge
```

```bash
$ docker exec -it sge_master qsub -b y -o /dask-drmaa/scratch/out.txt -e /dask-drmaa/scratch/err.txt /dask-drmaa/scratch/test.sh
$ docker exec -it slave_one cat /root/out.txt
```

Note: `dask-drmaa` is mounted with docker in each container.  To use a bash script or python file please give the full path to `/dask-drmaa/scratch/myfile.xyz`

## SGE Commands

- remove job: qdel -u <username>
- list jobs: qstat -f

