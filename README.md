
- remove job: qdel -u <username>
- list jobs: qstat -f

## Setup

```bash
$ docker-compose up
$ docker exec -it slave_one /etc/init.d/gridengine-exec restart
```

```bash
$ docker exec -it sge_master qsub -b y -o out.txt -e err.txt hostname
$ docker exec -it slave_one cat /root/out.txt
```
