import logging
import shlex
import subprocess

from common_functions import failing_workflow_UnknownError
from common_functions import full_workflow
from common_functions import full_workflow_TaskExecutionError
from common_functions import non_executable_task_command
from common_functions import PREFIX
from devtools import debug

from fractal_server.app.runner.executors.slurm.sudo._subprocess_run_as_user import (  # noqa
    _run_command_as_user,
)
from tests.fixtures_slurm import SLURM_USER


FRACTAL_RUNNER_BACKEND = "slurm"


def _reset_permissions_for_user_folder(folder):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    logging.warning(f"[_reset_permissions_for_user_folder] {folder=}")
    _run_command_as_user(
        cmd=f"chmod -R 777 {folder}",
        user=SLURM_USER,
        check=True,
    )


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
    fractal_tasks_mock_db,
    relink_python_interpreter_v2,  # before 'monkey_slurm' (#1462)
    monkey_slurm,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    project_dir = str(tmp777_path / "user_project_dir-slurm")

    await full_workflow(
        MockCurrentUser=MockCurrentUser,
        user_settings_dict=dict(
            slurm_user=SLURM_USER,
            slurm_accounts=[],
            project_dir=project_dir,
        ),
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
        tasks=fractal_tasks_mock_db,
    )
    _reset_permissions_for_user_folder(project_dir)


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
    fractal_tasks_mock_db,
    relink_python_interpreter_v2,  # before 'monkey_slurm' (#1462)
    monkey_slurm,
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

    project_dir = str(tmp777_path / "user_project_dir-slurm")

    await full_workflow_TaskExecutionError(
        MockCurrentUser=MockCurrentUser,
        user_settings_dict=dict(
            slurm_user=SLURM_USER,
            slurm_accounts=[],
            project_dir=project_dir,
        ),
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
        tasks=fractal_tasks_mock_db,
    )

    _reset_permissions_for_user_folder(project_dir)


# Tested with 'slurm' backend only.
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
    fractal_tasks_mock_db,
    relink_python_interpreter_v2,  # before 'monkey_slurm' (#1462)
    monkey_slurm,
    tmp_path,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR=tmp_path_factory.getbasetemp() / "FRACTAL_TASKS_DIR",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    project_dir = str(tmp777_path / "user_project_dir-slurm")
    user_kwargs = dict(is_verified=True)
    async with MockCurrentUser(
        user_kwargs=user_kwargs,
        user_settings_dict=dict(
            slurm_user=SLURM_USER,
            slurm_accounts=[],
            project_dir=project_dir,
        ),
    ) as user:
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
        task_id = fractal_tasks_mock_db["generic_task"].id

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
        slurm_user = SLURM_USER

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

    _reset_permissions_for_user_folder(project_dir)


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
    relink_python_interpreter_v2,  # before 'monkey_slurm' (#1462)
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

    project_dir = str(tmp777_path / "user_project_dir-slurm")

    await non_executable_task_command(
        MockCurrentUser=MockCurrentUser,
        user_settings_dict=dict(
            slurm_user=SLURM_USER,
            slurm_accounts=[],
            project_dir=project_dir,
        ),
        client=client,
        testdata_path=testdata_path,
        project_factory_v2=project_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        task_factory_v2=task_factory_v2,
    )

    _reset_permissions_for_user_folder(project_dir)


async def test_failing_workflow_UnknownError_slurm(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    request,
    override_settings_factory,
    monkeypatch,
    relink_python_interpreter_v2,  # before 'monkey_slurm' (#1462)
    monkey_slurm,
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

    project_dir = str(tmp777_path / "user_project_dir-slurm")

    await failing_workflow_UnknownError(
        MockCurrentUser=MockCurrentUser,
        user_settings_dict=dict(
            slurm_user=SLURM_USER,
            slurm_accounts=[],
            project_dir=project_dir,
        ),
        client=client,
        monkeypatch=monkeypatch,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
    )

    _reset_permissions_for_user_folder(project_dir)
