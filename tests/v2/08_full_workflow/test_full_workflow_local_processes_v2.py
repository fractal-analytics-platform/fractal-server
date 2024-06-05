import json
import shlex
import subprocess
import time
from concurrent.futures.process import BrokenProcessPool
from glob import glob
from pathlib import Path

import pytest
from common_functions import _task_name_to_id
from common_functions import failing_workflow_UnknownError
from common_functions import full_workflow
from common_functions import full_workflow_TaskExecutionError
from common_functions import non_executable_task_command
from common_functions import PREFIX
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.filenames import FILTERS_FILENAME
from fractal_server.app.runner.filenames import HISTORY_FILENAME
from fractal_server.app.runner.filenames import IMAGES_FILENAME
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.app.runner.v2._local_processes.executor import (
    FractalProcessPoolExecutor,
)

FRACTAL_RUNNER_BACKEND = "local_processes"


async def test_full_workflow_local(
    client,
    MockCurrentUser,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    fractal_tasks_mock,  # needed
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
        FRACTAL_LOCAL_CONFIG_FILE=tmp777_path / "local_config.json",
    )

    # Test invalid local config (LocalBackendConfigError)
    with open(tmp777_path / "local_config.json", "w") as f:
        json.dump(dict(foo=0), f)
    async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id,
            name="dataset",
        )
        workflow = await workflow_factory_v2(
            project_id=project.id, name="workflow"
        )

        res = await client.get(f"{PREFIX}/task/")
        task_id = _task_name_to_id("create_ome_zarr_compound", res.json())
        await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(image_dir="/foo", num_images=3)),
        )

        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{res.json()['id']}/"
        )
        assert res.json()["status"] == "failed"
        assert "LocalBackendConfigError" in res.json()["log"]

    # Test valid local config
    with open(tmp777_path / "local_config.json", "w") as f:
        json.dump(dict(parallel_tasks_per_job=10), f)
    await full_workflow(
        MockCurrentUser=MockCurrentUser,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
    )


async def test_full_workflow_TaskExecutionError(
    client,
    MockCurrentUser,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    fractal_tasks_mock,  # needed
):
    """ "
    Run a workflow made of three tasks, two successful tasks and one
    that raises an error.
    """

    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
    )
    await full_workflow_TaskExecutionError(
        MockCurrentUser=MockCurrentUser,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
    )


async def test_non_executable_task_command_local(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    task_factory_v2,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
):
    """
    Execute a workflow with a task which has an invalid `command` (i.e. it is
    not executable).
    """
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
    )
    await non_executable_task_command(
        MockCurrentUser=MockCurrentUser,
        client=client,
        testdata_path=testdata_path,
        project_factory_v2=project_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        task_factory_v2=task_factory_v2,
    )


@pytest.mark.parametrize("legacy", [False, True])
async def test_failing_workflow_UnknownError_local(
    legacy: bool,
    client,
    MockCurrentUser,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    task_factory,
    monkeypatch,
    override_settings_factory,
):
    """
    Submit a workflow that fails with some unrecognized exception (due
    to a monkey-patched function in the runner).
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
    )
    await failing_workflow_UnknownError(
        MockCurrentUser=MockCurrentUser,
        client=client,
        monkeypatch=monkeypatch,
        legacy=legacy,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory=task_factory,
        task_factory_v2=task_factory_v2,
    )


async def test_non_python_task_local(
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    testdata_path,
    override_settings_factory,
):
    """
    Run a full workflow with a single bash task, which simply writes
    something to stderr and stdout
    """
    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)

    async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:
        # Create project
        project = await project_factory_v2(user)
        project_id = project.id

        # Create workflow
        workflow = await workflow_factory_v2(
            name="test_wf", project_id=project_id
        )

        # Create task
        task = await task_factory_v2(
            name="non-python",
            source="custom-task",
            type="non_parallel",
            command_non_parallel=(
                f"bash {str(testdata_path)}/non_python_task_issue1377.sh"
            ),
        )

        # Add task to workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        assert res.status_code == 201

        # Create datasets
        dataset = await dataset_factory_v2(
            project_id=project_id, name="dataset"
        )

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check that the workflow execution is complete
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"
        debug(job_status_data["end_timestamp"])
        assert job_status_data["end_timestamp"]

        # Check that the expected files are present
        working_dir = job_status_data["working_dir"]
        glob_list = [Path(x).name for x in glob(f"{working_dir}/*")] + [
            Path(x).name for x in glob(f"{working_dir}/*/*")
        ]

        must_exist = [
            "0.log",
            "0.args.json",
            IMAGES_FILENAME,
            HISTORY_FILENAME,
            FILTERS_FILENAME,
            WORKFLOW_LOG_FILENAME,
        ]

        for f in must_exist:
            assert f in glob_list

        # Check that stderr and stdout are as expected
        with open(f"{working_dir}/0_non-python/0.log", "r") as f:
            log = f.read()
        assert "This goes to standard output" in log
        assert "This goes to standard error" in log


def _sleep_and_return(sleep_time):
    time.sleep(sleep_time)
    return 42


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


def wait_one_sec(*args, **kwargs):
    time.sleep(1)
    return 42


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
            executor.map(wait_one_sec, range(100))

    tmp_stdout.close()
    tmp_stderr.close()


def test_unit_map_iterables():
    with pytest.raises(ValueError) as error:
        with FractalProcessPoolExecutor(shutdown_file="/") as executor:
            executor.map(wait_one_sec, range(100), range(99))
    assert "Iterables have different lengths." in error._excinfo[1].args[0]
