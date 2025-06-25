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

import psutil

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
    # add more worker types here
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


def start_service(service_name: str):
    # for scheduler and triggerer (and dag-processor in future)
    # todo: add hostname for multiple schedulers/triggerers
    assert not check_service(service_name)

    pid_file = pid_file_location(service_name)
    subprocess.check_call(
        [
            "airflow",
            service_name,
            "--daemon",
            "--pid",
            str(pid_file),
            "--log-file",
            str(SERVICES_DIR / f"{service_name}.log"),
            "--stdout",
            str(SERVICES_DIR / f"{service_name}.out"),
            "--stderr",
            str(SERVICES_DIR / f"{service_name}.err"),
        ],
        cwd=str(AIRFLOW_HOME),
        env=os.environ | {"AIRFLOW_HOME": str(AIRFLOW_HOME)},
        stdout=subprocess.DEVNULL,
    )

    for _ in range(10):
        if pid_file.is_file():
            break
        time.sleep(0.1)
    else:
        print(f"start of {service_name} failed")


def stop_service(service_name: str):
    pid = check_service(service_name)

    if pid:
        proc = psutil.Process(pid)
        proc.terminate()
        proc.wait(10)

        if psutil.pid_exists(pid):
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
        if psutil.pid_exists(pid):
            return pid
        print("removing stale PID file")
        pid_file.unlink()


### handle DAG processor and api service specially


def start_dag_processor():
    # the DAG processor doesn't like the daemon mode, so starting with "nohup"

    service_name = "dag-processor"
    assert not check_service(service_name)

    new_proc = os.fork()

    if new_proc != 0:
        pid_file = pid_file_location(service_name)
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

    os.execvpe(
        file="airflow",
        args=[
            "airflow",
            "dag-processor",
            # probably doesn't do anything wo daemon mode
            "--log-file",
            str(SERVICES_DIR / f"{service_name}.log"),
        ],
        env=os.environ | {"AIRFLOW_HOME": str(AIRFLOW_HOME)},
    )
    # no return from here!


def start_api_server():
    # the API server doesn't write a PID file
    service_name = "api-server"

    assert not find_api_server()
    pid_file = pid_file_location(service_name)

    subprocess.check_call(
        [
            "airflow",
            "api-server",
            "--daemon",
            "--access-logfile",
            str(SERVICES_DIR / f"{service_name}.access"),
            # these don't do anything at the moment ?!
            "--pid",
            str(pid_file),
            "--log-file",
            str(SERVICES_DIR / f"{service_name}.log"),
            "--stdout",
            str(SERVICES_DIR / f"{service_name}.out"),
            "--stderr",
            str(SERVICES_DIR / f"{service_name}.err"),
        ],
        cwd=str(AIRFLOW_HOME),
        env=os.environ | {"AIRFLOW_HOME": str(AIRFLOW_HOME)},
        stdout=subprocess.DEVNULL,
    )

    for _ in range(10):
        # work around the lack of a pid file
        server_pid = find_api_server()
        if server_pid:
            with open(pid_file, mode="wt") as pid_file:
                pid_file.write(str(server_pid))
            return server_pid
        time.sleep(0.1)
    else:
        print(f"start of {service_name} failed")
        if pid_file.is_file():
            pid_file.unlink()
        return


def stop_api_server():

    pid = check_api_server()

    if pid:
        proc = psutil.Process(pid)
        proc.terminate()
        proc.wait(10)
        if psutil.pid_exists(pid):
            print("failed to end process")
            return

    pid_file = pid_file_location("api-server")
    if pid_file.is_file():
        pid_file.unlink()


def find_api_server() -> int | None:
    """
    find API server PID with psutil
    """
    for proc in psutil.process_iter():
        if proc.uids().real != os.getuid():
            continue
        proc_name = proc.name()
        if proc_name and proc_name.startswith("airflow api_server"):
            return proc.pid


def check_api_server() -> int | None:
    pid = check_service("api-server")
    if not pid:
        pid = find_api_server()
        if pid:
            pid_file = pid_file_location("api-server")
            with open(pid_file, mode="wt") as pid_file:
                pid_file.write(str(pid))

    return pid


### workers (per host/pool/queue)


def start_worker(worker_type="default"):
    # default or gpu

    service_name = f"worker"
    if worker_type != "default":
        service_name += f"-{worker_type}"

    # for scheduler and triggerer (and dag-processor in future)
    # todo: add hostname for multiple schedulers/triggerers
    assert not check_service(service_name)

    pid_file = pid_file_location(service_name, hostname=True)
    worker_start_args = [
        "airflow",
        "celery",
        "worker",
        "--daemon",
        "--pid",
        str(pid_file),
        "--log-file",
        str(SERVICES_DIR / f"{service_name}-{HOSTNAME}.log"),
        "--stdout",
        str(SERVICES_DIR / f"{service_name}-{HOSTNAME}.out"),
        "--stderr",
        str(SERVICES_DIR / f"{service_name}-{HOSTNAME}.err"),
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

    subprocess.check_call(
        worker_start_args,
        cwd=str(AIRFLOW_HOME),
        env=os.environ | {"AIRFLOW_HOME": str(AIRFLOW_HOME)},
        stdout=subprocess.DEVNULL,
    )

    for _ in range(10):
        if pid_file.is_file():
            return check_service(service_name, True)
        time.sleep(0.1)
    else:
        print(f"start of {service_name}@{HOSTNAME} failed")


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
        print(f"start of {service_name}@{HOSTNAME} failed")


### the CLI commands


def check_status(_):
    def report_status(service_name, pid):
        if pid:
            print(service_name, f"up (pid={pid})")
        else:
            print(service_name, "down")

    report_status("api-server", check_api_server())
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
        stop_api_server()
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
        start_api_server()
    if "scheduler" in services:
        start_service("scheduler")
    if "triggerer" in services:
        start_service("triggerer")
    if "dag-processor" in services:
        start_dag_processor()

    for service in services:
        if service.startswith("worker"):
            worker_type = service.removeprefix("worker").removeprefix("-")
            if not worker_type:
                start_worker()
            else:
                start_worker(worker_type)


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
