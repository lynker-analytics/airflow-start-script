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

The script works well in the same environment as the airflow installation (i.e it expects "airflow" on the `PATH`), it requires `psutil` and does not try to select other enviroments.

## Assumptions

We have a head/main node, which runs api-server, triggerer, scheduler and dag-processor. Those are assumed (for now) to run only there.

The worker nodes have the `AIRFLOW_HOME` directory mounted, so the configuration, DAGS and scripts are shared.

The celery executor distributes workloads and the [airflow conda operator](ttps://github.com/lynker-analytics/airflow-conda-operator/)
to coordinate different runtime environments.

Worker processes (celery based) can be started on each node and the log/pid file names include the nodes' hostname.

The script offers a work-around for:

* dag-processor doesn't like being daemonized [#50038](https://github.com/apache/airflow/issues/50038) - so we do the nohup in python and create the PID file.
* api-server doesn't write a PID file - so we find the PID with `psutil`.

So, please feel free to fork the repo/copy/cherry-pick what you like.
