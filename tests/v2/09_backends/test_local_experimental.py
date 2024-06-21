import json
import os
import re
import shlex
import subprocess
import threading
import time
from concurrent.futures.process import _ExecutorManagerThread
from concurrent.futures.process import BrokenProcessPool
from concurrent.futures.process import ProcessPoolExecutor

import pytest
from devtools import debug
from pydantic import BaseModel
from pydantic import Field

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.v2._local_experimental import _process_workflow
from fractal_server.app.runner.v2._local_experimental import process_workflow
from fractal_server.app.runner.v2._local_experimental._local_config import (
    get_local_backend_config,
)
from fractal_server.app.runner.v2._local_experimental._local_config import (
    LocalBackendConfigError,
)
from fractal_server.app.runner.v2._local_experimental.executor import (
    FractalProcessPoolExecutor,
)


class MockWorkflowTask(BaseModel):
    meta_non_parallel: dict = Field(default_factory=dict)
    meta_parallel: dict = Field(default_factory=dict)


def _sleep_and_return(sleep_time):
    time.sleep(sleep_time)
    return 42


def _wait_one_sec(*args, **kwargs):
    time.sleep(1)
    return 42


def two_args(a, b):
    return


async def test_unit_process_workflow():
    with pytest.raises(NotImplementedError):
        await process_workflow(
            workflow=None,
            dataset=None,
            logger_name=None,
            workflow_dir_local="/foo",
            workflow_dir_remote="/bar",
        )


async def test_get_local_backend_config(tmp_path):

    config_file = tmp_path / "config.json"
    valid_config = dict(parallel_tasks_per_job=1)
    invalid_config = dict(parallel_tasks_per_job=1, invalid_key=0)

    with config_file.open("w") as f:
        json.dump(invalid_config, f)
    with pytest.raises(LocalBackendConfigError):
        get_local_backend_config(
            wftask=MockWorkflowTask(),
            which_type="parallel",
            config_path=config_file,
        )

    with config_file.open("w") as f:
        json.dump(valid_config, f)

    get_local_backend_config(
        wftask=MockWorkflowTask(),
        which_type="parallel",
        config_path=config_file,
    )

    with pytest.raises(ValueError):
        get_local_backend_config(
            wftask=MockWorkflowTask(),
            which_type="not a valid type",
            config_path=config_file,
        )

    # test 'parallel_tasks_per_job' from wftask.meta_parallel
    get_local_backend_config(
        wftask=MockWorkflowTask(meta_parallel=dict(parallel_tasks_per_job=42)),
        which_type="parallel",
    )


def test_indirect_shutdown_during_submit(tmp_path):

    shutdown_file = tmp_path / "shutdown"
    with FractalProcessPoolExecutor(
        shutdown_file=str(shutdown_file)
    ) as executor:

        res = executor.submit(_sleep_and_return, 100)

        with shutdown_file.open("w"):
            pass
        assert shutdown_file.exists()

        time.sleep(2)

        assert isinstance(res.exception(), BrokenProcessPool)
        with pytest.raises(BrokenProcessPool):
            res.result()


def test_indirect_shutdown_during_map(
    tmp_path,
):
    shutdown_file = tmp_path / "shutdown"

    # NOTE: the executor.map call below is blocking. For this reason, we write
    # the shutdown file from a subprocess.Popen, so that we can make it happen
    # during the execution.
    shutdown_sleep_time = 2
    tmp_script = (tmp_path / "script.sh").as_posix()
    debug(tmp_script)
    with open(tmp_script, "w") as f:
        f.write(f"sleep {shutdown_sleep_time}\n")
        f.write(f"cat NOTHING > {shutdown_file.as_posix()}\n")

    tmp_stdout = open((tmp_path / "stdout").as_posix(), "w")
    tmp_stderr = open((tmp_path / "stderr").as_posix(), "w")

    subprocess.Popen(
        shlex.split(f"bash {tmp_script}"),
        stdout=tmp_stdout,
        stderr=tmp_stderr,
    )

    with pytest.raises(JobExecutionError):
        with FractalProcessPoolExecutor(
            shutdown_file=str(shutdown_file)
        ) as executor:
            executor.map(_wait_one_sec, range(100))

    tmp_stdout.close()
    tmp_stderr.close()


def test_unit_map_iterables():
    with pytest.raises(ValueError) as error:
        with FractalProcessPoolExecutor(shutdown_file="/") as executor:
            executor.map(two_args, range(100), range(99))
    assert "Iterables have different lengths." in error._excinfo[1].args[0]


async def test_indirect_shutdown_during_process_workflow(
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    db,
):
    """
    We test the try/except inside
    `fractal_server.app.runner.v2._local_experimental.py::_process_workflow`
    """

    async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(
            name="test_wf", project_id=project.id
        )
        with open(tmp_path / "foo.sh", "w") as f:
            f.write("sleep 4")
        os.mkdir(tmp_path / "0_task0")

        task = await task_factory_v2(
            type="non_parallel",
            command_non_parallel=f"bash {tmp_path / 'foo.sh'}",
        )
        await _workflow_insert_task(
            db=db, workflow_id=workflow.id, task_id=task.id
        )

        shutdown_file = tmp_path / SHUTDOWN_FILENAME

        tmp_script = (tmp_path / "script.sh").as_posix()
        with open(tmp_script, "w") as f:
            f.write("sleep 1\n")
            f.write(f"touch {shutdown_file.as_posix()}")

        tmp_stdout = open((tmp_path / "stdout").as_posix(), "w")
        tmp_stderr = open((tmp_path / "stderr").as_posix(), "w")

        with pytest.raises(JobExecutionError):
            subprocess.Popen(
                shlex.split(f"bash {tmp_script}"),
                stdout=tmp_stdout,
                stderr=tmp_stderr,
            )
            _process_workflow(
                workflow=workflow,
                dataset=dataset,
                logger_name="logger",
                workflow_dir_local=tmp_path,
                first_task_index=0,
                last_task_index=0,
            )
        tmp_stdout.close()
        tmp_stderr.close()


def test_count_threads_and_processes(tmp_path):

    shutdown_file = tmp_path / "shutdown"

    # --- Threads
    initial_threads = threading.enumerate()
    # `len(initial_threads)` == 1 when the test is run on its own
    # `len(initial_threads)` == 2 when the test is run on its own
    # `len(initial_threads)` == 2 when the test is run on GitHub
    assert len(initial_threads) in [1, 2, 3]
    assert initial_threads[0].name == "MainThread"
    if len(initial_threads) > 1:
        assert initial_threads[1].name == "asyncio_0"

    # our `FractalProcessPoolExecutor`
    MAX_WORKERS = 3
    with FractalProcessPoolExecutor(
        shutdown_file=str(shutdown_file), max_workers=MAX_WORKERS
    ) as executor:

        # --- Threads
        threads = threading.enumerate()
        assert len(threads) == len(initial_threads) + 1
        assert threads[:-1] == initial_threads

        thread_1 = threads[-1]

        # Names are assigned sequentially to threads, so the names of our
        # threads depend on how many tests we are running.
        T = int(re.match(r"^Thread-(\d+) \(_run\)$", thread_1.name).group(1))
        assert thread_1.name == f"Thread-{T} (_run)"

        assert not isinstance(thread_1, _ExecutorManagerThread)
        assert thread_1.daemon is True
        assert thread_1.is_alive() is True

        # --- Processes
        assert executor._processes == dict()

        # +++ FIRST SUBMIT
        executor.submit(_sleep_and_return, 5)

        # --- Threads
        threads = threading.enumerate()
        assert len(threads) == len(initial_threads) + 3
        assert threads[:-2] == initial_threads + [thread_1]

        thread_2 = threads[-2]
        assert thread_2.name == f"Thread-{T+1}"
        assert isinstance(thread_2, _ExecutorManagerThread)
        assert thread_2.daemon is False
        assert thread_2.is_alive() is True

        thread_3 = threads[-1]
        assert thread_3.name == "QueueFeederThread"
        assert not isinstance(thread_3, _ExecutorManagerThread)
        assert thread_3.daemon is True

        # --- Processes
        assert len(executor._processes) == 1
        process_1 = next(iter(executor._processes.values()))
        # Same as threads
        P = int(re.match(r"^SpawnProcess-(\d+)$", process_1.name).group(1))
        assert process_1.name == f"SpawnProcess-{P}"
        assert process_1.is_alive() is True
        assert process_1.daemon is False

        # +++ SECOND SUBMIT
        executor.submit(_sleep_and_return, 5)

        # --- Threads
        threads = threading.enumerate()
        assert threads == initial_threads + [thread_1, thread_2, thread_3]

        # --- Processes
        assert len(executor._processes) == 2
        assert executor._processes[process_1.pid] == process_1

        process_2 = executor._processes[
            next(
                pid
                for pid in executor._processes.keys()
                if pid not in [process_1.pid]
            )
        ]
        assert process_2.name == f"SpawnProcess-{P+1}"
        assert process_2.is_alive() is True
        assert process_2.daemon is False

        for _ in range(MAX_WORKERS + 5):
            # There is a limit on number of processes
            executor.submit(_sleep_and_return, 5)
        assert len(executor._processes) == MAX_WORKERS

        # +++ SHUTDOWN
        with shutdown_file.open("w"):
            pass
        assert shutdown_file.exists()
        time.sleep(2)

        # --- Threads
        threads = threading.enumerate()
        assert threads == initial_threads

        assert thread_1.is_alive() is False
        assert thread_2.is_alive() is False
        assert thread_3.is_alive() is False

        # --- Processes
        assert len(executor._processes) == MAX_WORKERS
        for process in executor._processes.values():
            assert process.is_alive() is False

    # --- Threads
    threads = threading.enumerate()
    assert threads == initial_threads
    # --- Processes
    assert executor._processes is None

    # `concurrent.futures.process.ProcessPoolExecutor`
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # --- Threads
        threads = threading.enumerate()
        # there is no " (_run)" thread here
        assert threads == initial_threads

        # --- Processes
        assert executor._processes == dict()

        # +++ FIRST SUBMIT
        executor.submit(_sleep_and_return, 2)

        # --- Threads
        threads = threading.enumerate()
        assert len(threads) == len(initial_threads) + 2
        assert threads[:-2] == initial_threads

        thread_4 = threads[-2]
        assert thread_4.name == f"Thread-{T+2}"
        assert isinstance(thread_4, _ExecutorManagerThread)
        assert thread_4.daemon is False
        assert thread_4.is_alive() is True

        thread_5 = threads[-1]
        assert thread_5.name == "QueueFeederThread"
        assert not isinstance(thread_5, _ExecutorManagerThread)
        assert thread_5.daemon is True

        # --- Processes
        assert len(executor._processes) == 1
        process_3 = next(iter(executor._processes.values()))
        # `process_3` name is not `SpawnProcess-{P+2}`
        # It means that somewhere the `SpawnProcess-{P+2}` has been run
        assert process_3.name == f"SpawnProcess-{P+3}"
        assert process_3.is_alive() is True
        assert process_3.daemon is False

        # +++ SECOND SUBMIT
        executor.submit(_sleep_and_return, 2)

        # --- Threads
        threads = threading.enumerate()
        assert threads == initial_threads + [thread_4, thread_5]

        # --- Processes
        assert len(executor._processes) == 2
        assert executor._processes[process_3.pid] == process_3

        process_4 = executor._processes[
            next(
                pid
                for pid in executor._processes.keys()
                if pid not in [process_3.pid]
            )
        ]
        assert process_4.name == f"SpawnProcess-{P+4}"
        assert process_4.is_alive() is True
        assert process_4.daemon is False

        for _ in range(MAX_WORKERS + 5):
            # There is a limit on number of processes
            executor.submit(_sleep_and_return, 1)
        assert len(executor._processes) == MAX_WORKERS

    threads = threading.enumerate()
    assert threads == initial_threads
    assert thread_4.is_alive() is False
    assert thread_5.is_alive() is False

    assert executor._processes is None
