import json
import multiprocessing as mp
import os
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
        process_workflow(
            workflow=None,
            dataset=None,
            logger_name=None,
            workflow_dir_local="/foo",
            workflow_dir_remote="/bar",
            job_attribute_filters={},
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
            user_id=user.id,
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
            process_workflow(
                workflow=workflow,
                dataset=dataset,
                logger_name="logger",
                workflow_dir_local=tmp_path,
                first_task_index=0,
                last_task_index=0,
                job_attribute_filters={},
            )
        tmp_stdout.close()
        tmp_stderr.close()


def test_count_threads_and_processes(tmp_path):

    shutdown_file = tmp_path / "shutdown"

    # --- Threads
    initial_threads = threading.enumerate()
    assert len(initial_threads) in [1, 2, 3]
    # `len(initial_threads)` == 1 when the test is run on its own:
    #   - MainThread
    # `len(initial_threads)` == 2 when the test is run on a suite of tests:
    #   - MainThread
    #   - asyncio_0
    # `len(initial_threads)` == 3 when the test is run on GitHub CI:
    #   - MainThread
    #   - asyncio_0
    #   - Thread-{N}
    #
    # NOTE: In the third case, it is confusing that Thread-{N} sometimes goes
    # from started to stopped and we don't know why

    # our `FractalProcessPoolExecutor`
    with FractalProcessPoolExecutor(
        shutdown_file=str(shutdown_file)
    ) as executor:

        # --- Threads
        threads = threading.enumerate()
        # This is our `_shutdown_file_thread`
        assert len(threads) == len(initial_threads) + 1
        assert not isinstance(threads[-1], _ExecutorManagerThread)

        # --- Processes
        assert executor._processes == dict()

        # +++ SUBMITS
        for _ in range(executor._max_workers + 10):
            executor.submit(_sleep_and_return, 5)

        # --- Threads
        threads = threading.enumerate()
        assert len(threads) == len(initial_threads) + 3
        executor_threads = threads[-3:]

        assert threads[-2].name.startswith("Thread-")
        assert isinstance(threads[-2], _ExecutorManagerThread)

        assert threads[-1].name == "QueueFeederThread"
        assert not isinstance(threads[-1], _ExecutorManagerThread)

        # --- Processes
        assert len(executor._processes) == executor._max_workers
        for process in executor._processes.values():
            assert process.is_alive() is True

        # +++ SHUTDOWN
        with shutdown_file.open("w"):
            pass
        assert shutdown_file.exists()
        time.sleep(2)

        # --- Threads
        current_threads = threading.enumerate()
        for thread in executor_threads:
            assert thread not in current_threads

        # --- Processes
        assert len(executor._processes) == executor._max_workers
        for process in executor._processes.values():
            assert process.is_alive() is False

    # --- Threads
    new_initial_threads = threading.enumerate()
    for thread in executor_threads:
        assert thread not in new_initial_threads

    # --- Processes
    assert executor._processes is None

    # `concurrent.futures.process.ProcessPoolExecutor`
    with ProcessPoolExecutor(mp_context=mp.get_context("spawn")) as executor:
        # --- Threads
        threads = threading.enumerate()
        assert threads == new_initial_threads

        # --- Processes
        assert executor._processes == dict()

        # +++ SUBMITS
        for _ in range(executor._max_workers + 10):
            # There is a limit on number of processes
            executor.submit(_sleep_and_return, 0.2)

        # --- Threads
        threads = threading.enumerate()
        assert len(threads) == len(new_initial_threads) + 2

        assert threads[-2].name.startswith("Thread-")
        assert isinstance(threads[-2], _ExecutorManagerThread)

        assert threads[-1].name == "QueueFeederThread"
        assert not isinstance(threads[-1], _ExecutorManagerThread)

        # --- Processes
        assert len(executor._processes) == executor._max_workers
        for process in executor._processes.values():
            assert process.is_alive() is True

    # --- Threads
    threads = threading.enumerate()
    assert threads == new_initial_threads
    # --- Processes
    assert executor._processes is None
