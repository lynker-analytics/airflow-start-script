# airflow-start-script
a start/stop/status script working with airflow 3.0.x

For our on-premise cluster we needed a reasonably robust (ad-hoc) start / stop script which works with airflow 3.

```bash
$ python airflow_services.py status
api-server up (pid=2943991)
scheduler up (pid=2944014)
triggerer up (pid=2944034)
dag-processor down
worker@aber up (pid=2944221)
worker-gpu@aber up (pid=2944283)
```

Selective start (or stop)
```bash
$ python airflow_services.py start dag-processor
```

Start (or stop) core services (api-server, scheduler, triggerer, dag-processor):

```bash
$ python airflow_services.py start
```

A little bit of help:

```bash
$ python airflow_services.py start --help
usage: airflow_services.py start [-h] [{api-server,scheduler,triggerer,dag-processor,worker,worker-gpu} ...]

positional arguments:
  {api-server,scheduler,triggerer,dag-processor,worker,worker-gpu}

options:
  -h, --help            show this help message and exit
```

We have a head/main node, which runs api-server, triggerer, scheduler and dag-processor.

The worker nodes have the `AIRFLOW_HOME` directory mounted, so the configuration, DAGS and scripts are shared.

The celery executor distributes workloads and the [airflow conda operator](ttps://github.com/lynker-analytics/airflow-conda-operator/)
to coordinate different runtime environments.

Worker processes (celery based) can be started on each node and the log/pid files are prefixed by the node hostnames.

The script offers a work-around for:

* dag-processor doesn't like being daemonized [#50038](https://github.com/apache/airflow/issues/50038) - so we do the nohup in python and create the PID file.
* api-server doesn't write a PID file - so we find the PID with `psutil`.

So, please feel free to fork the repo/copy/cherry-pick what you like.
