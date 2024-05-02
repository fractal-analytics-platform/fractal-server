import shlex
import subprocess

import pytest
from common_functions import _task_name_to_id
from common_functions import failing_workflow_UnknownError
from common_functions import full_workflow
from common_functions import full_workflow_TaskExecutionError
from common_functions import non_executable_task_command
from common_functions import non_python_task
from common_functions import PREFIX
from devtools import debug


FRACTAL_RUNNER_BACKEND = "slurm"


async def test_full_workflow_slurm(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    monkey_slurm,
    relink_python_interpreter_v2,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    await full_workflow(
        MockCurrentUser=MockCurrentUser,
        user_kwargs={"cache_dir": str(tmp777_path / "user_cache_dir-slurm")},
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
    )


async def test_full_workflow_TaskExecutionError_slurm(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    monkey_slurm,
    relink_python_interpreter_v2,
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
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )
    await full_workflow_TaskExecutionError(
        MockCurrentUser=MockCurrentUser,
        user_kwargs={"cache_dir": str(tmp777_path / "user_cache_dir-slurm")},
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
    )


# only slurm
async def test_failing_workflow_JobExecutionError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    monkey_slurm_user,
    monkey_slurm,
    relink_python_interpreter_v2,
    tmp_path,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    user_cache_dir = str(tmp777_path / "user_cache_dir-slurm")
    user_kwargs = dict(cache_dir=user_cache_dir, is_verified=True)
    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        project = await project_factory_v2(user)
        project_id = project.id
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
        )
        dataset_id = dataset.id

        # Create workflow
        workflow = await workflow_factory_v2(
            name="test_wf", project_id=project_id
        )
        workflow_id = workflow.id

        # Retrieve relevant task ID
        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        task_list = res.json()
        task_id = _task_name_to_id(
            task_name="generic_task", task_list=task_list
        )

        # Add a short task, which will be run successfully
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(sleep_time=0.1)),
        )
        assert res.status_code == 201
        wftask0_id = res.json()["id"]

        # Add a long task, which will be stopped while running
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(sleep_time=200)),
        )
        assert res.status_code == 201
        wftask1_id = res.json()["id"]

        # NOTE: the client.post call below is blocking, due to the way we are
        # running tests. For this reason, we call the scancel function from a
        # from a subprocess.Popen, so that we can make it happen during the
        # execution.
        scancel_sleep_time = 10
        slurm_user = monkey_slurm_user

        tmp_script = (tmp_path / "script.sh").as_posix()
        debug(tmp_script)
        with open(tmp_script, "w") as f:
            f.write(f"sleep {scancel_sleep_time}\n")
            f.write(
                (
                    f"sudo --non-interactive -u {slurm_user} "
                    f"scancel -u {slurm_user} -v"
                    "\n"
                )
            )

        tmp_stdout = open((tmp_path / "stdout").as_posix(), "w")
        tmp_stderr = open((tmp_path / "stderr").as_posix(), "w")
        subprocess.Popen(
            shlex.split(f"bash {tmp_script}"),
            stdout=tmp_stdout,
            stderr=tmp_stderr,
        )

        # Submit the workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?{workflow_id=}&{dataset_id=}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202
        job_id = job_data["id"]
        debug(job_id)

        # Query status of the job
        rs = await client.get(f"{PREFIX}/project/{project_id}/job/{job_id}/")
        assert rs.status_code == 200
        job_status_data = rs.json()
        debug(job_status_data)
        print(job_status_data["log"])
        assert job_status_data["status"] == "failed"
        assert job_status_data["end_timestamp"]
        assert "id: None" not in job_status_data["log"]
        assert "JOB ERROR" in job_status_data["log"]
        assert "CANCELLED" in job_status_data["log"]
        assert "\\n" not in job_status_data["log"]

        # Test get_workflowtask_status endpoint
        res = await client.get(
            (
                f"{PREFIX}/project/{project_id}/status/?"
                f"dataset_id={dataset_id}&workflow_id={workflow_id}"
            )
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert statuses == {
            str(wftask0_id): "done",
            str(wftask1_id): "failed",
        }

        tmp_stdout.close()
        tmp_stderr.close()


async def test_non_executable_task_command_slurm(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    task_factory_v2,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    relink_python_interpreter_v2,
    monkey_slurm,
):
    """
    Execute a workflow with a task which has an invalid `command` (i.e. it is
    not executable).
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    await non_executable_task_command(
        MockCurrentUser=MockCurrentUser,
        user_kwargs={"cache_dir": str(tmp777_path / "user_cache_dir-slurm")},
        client=client,
        testdata_path=testdata_path,
        project_factory_v2=project_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        task_factory_v2=task_factory_v2,
    )


@pytest.mark.parametrize("legacy", [False, True])
async def test_failing_workflow_UnknownError_slurm(
    legacy: bool,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    task_factory,
    request,
    override_settings_factory,
    monkeypatch,
    monkey_slurm,
    relink_python_interpreter_v2,
):
    """
    Submit a workflow that fails with some unrecognized exception (due
    to a monkey-patched function in the runner).
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    await failing_workflow_UnknownError(
        MockCurrentUser=MockCurrentUser,
        user_kwargs={"cache_dir": str(tmp777_path / "user_cache_dir-slurm")},
        client=client,
        monkeypatch=monkeypatch,
        legacy=legacy,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory=task_factory,
        task_factory_v2=task_factory_v2,
    )


async def test_non_python_task_slurm(
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    testdata_path,
    tmp_path,
    override_settings_factory,
    tmp777_path,
    relink_python_interpreter_v2,
    monkey_slurm,
):
    """
    Run a full workflow with a single bash task, which simply writes
    something to stderr and stdout
    """
    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    await non_python_task(
        client=client,
        MockCurrentUser=MockCurrentUser,
        user_kwargs={"cache_dir": str(tmp777_path / "user_cache_dir-slurm")},
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
        testdata_path=testdata_path,
        tmp_path=tmp_path,
    )
