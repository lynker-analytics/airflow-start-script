# airflow-start-script
a start/stop/status script working with airflow 3.0.x

For our on-premise cluster we needed a reasonably robust (ad-hoc) start / stop script which works with airflow 3.
It manages all services from the CLI and assures the PID files are up to date.

```bash
$ python airflow_services.py status
api-server up (pid=2943991)
scheduler up (pid=2944014)
triggerer up (pid=2944034)
dag-processor down
worker@aber up (pid=2944221)
worker-gpu@aber up (pid=2944283)
```

Selective start (or stop) services by their name:

```bash
$ python airflow_services.py start dag-processor
```

Start (or stop) core services (api-server, scheduler, triggerer, dag-processor) at once:

```bash
$ python airflow_services.py start
```

Start a GPU celery queue on a worker (i.e. only one process at the time):

```bash
$ python airflow_services.py start worker-gpu
```

The stop command will use `airflow celery stop` to stop the worker gracefully.

A little bit of help:

```bash
$ python airflow_services.py start --help
usage: airflow_services.py start [-h] [{api-server,scheduler,triggerer,dag-processor,worker,worker-gpu} ...]

positional arguments:
  {api-server,scheduler,triggerer,dag-processor,worker,worker-gpu}

options:
  -h, --help            show this help message and exit
```

The script works well in the same environment as the airflow installation (i.e it expects "airflow" on the `PATH`).

PID and log files will be written into `$AIRFLOW_HOME/service-logs`.

The script does not use the "daemon mode" as this lead to problems with the `epoll` method of the `select` module for the dag-processor service and the celery workers.
Each service process is started with a `nohup` like procedure.

## Assumptions

We have a head/main node, which runs api-server, triggerer, scheduler and dag-processor. Those are assumed (for now) to run only there.

The worker nodes have the `AIRFLOW_HOME` directory mounted, so the configuration, DAGS and scripts are shared.

The celery executor distributes workloads and the [airflow conda operator](ttps://github.com/lynker-analytics/airflow-conda-operator/)
to manage the different runtime environments on each host.

Worker processes (celery based) can be started on each node and the log/pid file names include the nodes' hostname.

So, please feel free to fork the repo/copy/cherry-pick what you like.
