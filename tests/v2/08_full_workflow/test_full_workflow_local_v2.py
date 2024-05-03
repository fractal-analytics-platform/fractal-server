from glob import glob
from pathlib import Path

import pytest
from common_functions import failing_workflow_UnknownError
from common_functions import full_workflow
from common_functions import full_workflow_TaskExecutionError
from common_functions import non_executable_task_command
from common_functions import PREFIX
from devtools import debug

from fractal_server.app.runner.filenames import FILTERS_FILENAME
from fractal_server.app.runner.filenames import HISTORY_FILENAME
from fractal_server.app.runner.filenames import IMAGES_FILENAME
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME

FRACTAL_RUNNER_BACKEND = "local"


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
    )
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


# Tested with 'local' backend only.
# With 'slurm' backend we get:
#   TaskExecutionError: bash: /home/runner/work/fractal-server/fractal-server/
#       tests/data/non_python_task_issue1377.sh: No such file or directory
async def test_non_python_task_local(
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    testdata_path,
):
    """
    Run a full workflow with a single bash task, which simply writes
    something to stderr and stdout
    """
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
        glob_list = [Path(x).name for x in glob(f"{working_dir}/*")]
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
        with open(f"{working_dir}/0.log", "r") as f:
            log = f.read()
        assert "This goes to standard output" in log
        assert "This goes to standard error" in log
