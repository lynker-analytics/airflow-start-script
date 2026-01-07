"""
start/stop/status of airflow services

* api-server
* dag processor
* scheduler
* triggerer
* worker (gpu/default/...) on various worker nodes
"""

import argparse
import os
import pathlib
import signal
import socket
import subprocess
import time
import errno


AIRFLOW_HOME = pathlib.Path(
    os.environ.get("AIRFLOW_HOME", pathlib.Path.home() / "airflow")
)
assert AIRFLOW_HOME.is_dir()

SERVICES_DIR = AIRFLOW_HOME / "services-logs"
SERVICES_DIR.mkdir(exist_ok=True)

HOSTNAME = socket.gethostname()

ALL_SERVICES = [
    "api-server",
    "scheduler",
    "triggerer",
    "dag-processor",
    # add more worker types here?
    "worker",
    "worker-gpu",
]


def pid_file_location(
    service_name: str, hostname: str | bool | None = None
) -> pathlib.Path:

    hostname_suffix = ""
    if isinstance(hostname, str):
        hostname_suffix = f"-{hostname}"
    if hostname:
        hostname_suffix = f"-{HOSTNAME}"

    return SERVICES_DIR / f"{service_name}{hostname_suffix}.pid"


def check_pid_exists(pid: int) -> int | None:
    """
    Test whether the process id still exists (and permission to send signals to it).

    :param pid: process id
    :type pid: int
    :return: returns process id if process still exists, otherwise None
    :rtype: int | None
    """
    try:
        os.kill(pid, 0)
    except OSError as e:
        if e.errno == errno.ESRCH:
            return  # No such process
        raise
    return pid  # still there


def wait_pid_timeout(pid: int, timeout_seconds: float) -> int | None:
    """
    Waits for a process ID (pid) to stop existing with a timeout.
    Returns None if pid doesn't exist, otherwise pid.
    """
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        if check_pid_exists(pid) is None:
            return

        # Sleep briefly to avoid busy-waiting
        time.sleep(0.1)

    return check_pid_exists(pid)


def stop_service(service_name: str):
    pid = check_service(service_name)

    if pid:
        os.kill(pid, signal.SIGTERM)

        if wait_pid_timeout(pid, 10) is not None:
            print("failed to end process")
            return

        pid_file = pid_file_location(service_name)
        if pid_file.is_file():
            pid_file.unlink()


def check_service(service_name: str, hostname=None):
    pid_file = pid_file_location(service_name, hostname=hostname)
    if pid_file.is_file():
        with open(pid_file, mode="rt") as pid_text:
            pid = int(pid_text.readline().strip())

        if check_pid_exists(pid) is None:
            print("removing stale PID file")
            pid_file.unlink()
            return

        return pid


def start_service(service_name: str, start_args=None, add_hostname=None):
    assert not check_service(service_name)

    new_proc = os.fork()

    if new_proc != 0:
        pid_file = pid_file_location(service_name, add_hostname)
        with open(pid_file, mode="wt") as pid_text:
            pid_text.write(str(new_proc))

        return new_proc

    # nohup
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    os.chdir(str(AIRFLOW_HOME))

    # low level out/err stream redirect
    out_fd = os.open(
        str(SERVICES_DIR / f"{service_name}.out"),
        flags=os.O_WRONLY | os.O_CREAT,
    )
    os.dup2(out_fd, 1)

    err_fd = os.open(
        str(SERVICES_DIR / f"{service_name}.err"),
        flags=os.O_WRONLY | os.O_CREAT,
    )
    os.dup2(err_fd, 2)

    if start_args is None:
        start_args = [service_name]

    os.execvpe(
        file="airflow",
        args=[
            "airflow",
            *start_args,
            # probably doesn't do anything wo daemon mode
            "--log-file",
            str(SERVICES_DIR / f"{service_name}.log"),
        ],
        env=os.environ | {"AIRFLOW_HOME": str(AIRFLOW_HOME)},
    )
    # no return from here!


### workers (per host/pool/queue)


def start_worker(worker_type="default"):
    # default or gpu

    service_name = f"worker"
    if worker_type != "default":
        service_name += f"-{worker_type}"

    # for scheduler and triggerer (and dag-processor in future)
    # todo: add hostname for multiple schedulers/triggerers
    assert not check_service(service_name)

    worker_start_args = [
        "celery",
        "worker",
    ]
    if worker_type == "default":
        worker_start_args.extend(
            [
                "--celery-hostname",
                HOSTNAME,
            ]
        )
    elif worker_type == "gpu":
        worker_start_args.extend(
            [
                "--queues",
                "gpu",
                "--celery-hostname",
                f"{HOSTNAME}-gpu",
                "--concurrency",
                "1",
            ]
        )
    else:
        raise ValueError(f"unexpected worker type {worker_type}")

    start_service(service_name, worker_start_args, add_hostname=True)

    pid_file = pid_file_location(service_name, hostname=True)
    for _ in range(10):
        if pid_file.is_file():
            return check_service(service_name, True)
        time.sleep(0.1)
    else:
        print(f"failed to start {service_name}@{HOSTNAME}")


def stop_worker(worker_type="default"):
    # default or gpu

    service_name = f"worker"
    if worker_type != "default":
        service_name += f"-{worker_type}"

    pid = check_service(service_name, True)
    if not pid:
        return

    pid_file = pid_file_location(service_name, True)

    worker_stop_args = ["airflow", "celery", "stop", "--pid", str(pid_file)]

    subprocess.check_call(
        worker_stop_args,
        cwd=str(AIRFLOW_HOME),
        env=os.environ | {"AIRFLOW_HOME": str(AIRFLOW_HOME)},
        stdout=subprocess.DEVNULL,
    )

    # wait for the PID file to go away
    for _ in range(100):
        if not pid_file.is_file():
            break
        time.sleep(0.1)
    else:
        print(f"failed to stop {service_name}@{HOSTNAME}")


### the CLI commands


def check_status(args=None):
    def report_status(service_name, pid):
        if pid is not None:
            print(f"{service_name} up (pid={pid})")
        else:
            print(f"{service_name} down")

    report_status("api-server", check_service("api-server"))
    report_status("scheduler", check_service("scheduler"))
    report_status("triggerer", check_service("triggerer"))
    report_status("dag-processor", check_service("dag-processor"))

    report_status(f"worker@{HOSTNAME}", check_service("worker", True))
    report_status(f"worker-gpu@{HOSTNAME}", check_service("worker-gpu", True))


def stop(args):
    services = args.services
    if not services:
        services = ["api-server", "scheduler", "triggerer", "dag-processor"]

    if "api-server" in services:
        stop_service("api-server")
    if "scheduler" in services:
        stop_service("scheduler")
    if "triggerer" in services:
        stop_service("triggerer")
    if "dag-processor" in services:
        stop_service("dag-processor")

    for service in services:
        if service.startswith("worker"):
            worker_type = service.removeprefix("worker").removeprefix("-")
            if not worker_type:
                stop_worker()
            else:
                stop_worker(worker_type)


def start(args):
    services = args.services
    if not services:
        services = ["api-server", "scheduler", "triggerer", "dag-processor"]

    if "api-server" in services:
        start_service("api-server")
    if "scheduler" in services:
        start_service("scheduler")
    if "triggerer" in services:
        start_service("triggerer")
    if "dag-processor" in services:
        start_service("dag-processor")

    for service in services:
        if service.startswith("worker"):
            worker_type = service.removeprefix("worker").removeprefix("-")
            if not worker_type:
                start_worker()
            else:
                start_worker(worker_type)

    check_status()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    parser_status = subparsers.add_parser("status")
    parser_status.set_defaults(cmd_func=check_status)

    parser_stop = subparsers.add_parser("stop")
    parser_stop.add_argument(
        "services", action="extend", nargs="*", type=str, choices=ALL_SERVICES
    )
    parser_stop.set_defaults(cmd_func=stop)

    parser_start = subparsers.add_parser("start")
    parser_start.add_argument(
        "services", action="extend", nargs="*", type=str, choices=ALL_SERVICES
    )
    parser_start.set_defaults(cmd_func=start)

    # do as the commandline commands
    args = parser.parse_args()
    args.cmd_func(args)
