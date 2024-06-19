import json
import os
import shlex
import subprocess
import time
from concurrent.futures.process import BrokenProcessPool

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

    with pytest.raises(JobExecutionError):
        subprocess.Popen(
            shlex.split(f"bash {tmp_script}"),
            stdout=tmp_stdout,
            stderr=tmp_stderr,
        )

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
