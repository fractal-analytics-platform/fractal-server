import logging

import pytest
from sqlalchemy import select

from fractal_server.app.models.v2 import AccountingRecordSlurm
from fractal_server.app.runner.executors.slurm_sudo._subprocess_run_as_user import (  # noqa
    _run_command_as_user,
)
from tests.fixtures_slurm import SLURM_USER
from tests.v2.test_07_full_workflow.common_functions import (
    failing_workflow_UnknownError,
)
from tests.v2.test_07_full_workflow.common_functions import full_workflow
from tests.v2.test_07_full_workflow.common_functions import (
    full_workflow_TaskExecutionError,
)
from tests.v2.test_07_full_workflow.common_functions import (
    non_executable_task_command,
)


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


@pytest.mark.container
@pytest.mark.fails_on_macos
async def test_full_workflow_slurm(
    db,
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

    # Assert that SLURM job IDs were recorded
    res = await db.execute(
        select(AccountingRecordSlurm).order_by(AccountingRecordSlurm.timestamp)
    )
    slurm_records = res.scalars().all()
    assert len(slurm_records) == 5
    assert len(slurm_records[0].slurm_job_ids) == 1


@pytest.mark.container
@pytest.mark.fails_on_macos
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


@pytest.mark.container
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


@pytest.mark.container
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
