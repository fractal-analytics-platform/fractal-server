from tests.v2.test_07_full_workflow.common_functions import (
    failing_workflow_post_task_execution,
)
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
from tests.v2.test_07_full_workflow.common_functions import (
    workflow_with_non_python_task,
)

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
    fractal_tasks_mock_db,
):
    # Use a session-scoped FRACTAL_TASKS_DIR_zzz folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR_zzz=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR_zzz=tmp_path_factory.getbasetemp()
        / "FRACTAL_TASKS_DIR_zzz",
    )
    await full_workflow(
        MockCurrentUser=MockCurrentUser,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
        tasks=fractal_tasks_mock_db,
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
    fractal_tasks_mock_db,
):
    """ "
    Run a workflow made of three tasks, two successful tasks and one
    that raises an error.
    """

    # Use a session-scoped FRACTAL_TASKS_DIR_zzz folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR_zzz=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR_zzz=tmp_path_factory.getbasetemp()
        / "FRACTAL_TASKS_DIR_zzz",
    )
    await full_workflow_TaskExecutionError(
        MockCurrentUser=MockCurrentUser,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
        tasks=fractal_tasks_mock_db,
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
        FRACTAL_RUNNER_WORKING_BASE_DIR_zzz=tmp777_path / "artifacts",
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


async def test_failing_workflow_UnknownError_local(
    client,
    MockCurrentUser,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    monkeypatch,
    override_settings_factory,
):
    """
    Submit a workflow that fails with some unrecognized exception (due
    to a monkey-patched function in the runner).
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR_zzz=tmp777_path / "artifacts",
    )
    await failing_workflow_UnknownError(
        MockCurrentUser=MockCurrentUser,
        client=client,
        monkeypatch=monkeypatch,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
    )


# Tested with 'local' backends only.
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
    tmp777_path,
):
    """
    Run a full workflow with a single bash task, which simply writes
    something to stderr and stdout
    """
    await workflow_with_non_python_task(
        client=client,
        MockCurrentUser=MockCurrentUser,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
        testdata_path=testdata_path,
        tmp777_path=tmp777_path,
    )


async def test_failing_workflow_post_task_execution(
    client,
    MockCurrentUser,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    override_settings_factory,
    tmp_path_factory,
    tmp_path,
    fractal_tasks_mock_db,
):
    # Use a session-scoped FRACTAL_TASKS_DIR_zzz folder
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND,
        FRACTAL_RUNNER_WORKING_BASE_DIR_zzz=tmp777_path / "artifacts",
        FRACTAL_TASKS_DIR_zzz=tmp_path_factory.getbasetemp()
        / "FRACTAL_TASKS_DIR_zzz",
    )

    await failing_workflow_post_task_execution(
        MockCurrentUser=MockCurrentUser,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        client=client,
        tasks=fractal_tasks_mock_db,
        tmp_path=tmp_path,
    )
