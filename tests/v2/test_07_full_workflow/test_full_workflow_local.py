from fractal_server.app.schemas.v2 import ResourceType
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

FRACTAL_RUNNER_BACKEND = ResourceType.LOCAL


async def test_full_workflow_local(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    override_settings_factory,
    fractal_tasks_mock_db,
    local_resource_profile_db,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    resource, profile = local_resource_profile_db
    await full_workflow(
        MockCurrentUser=MockCurrentUser,
        project_factory=project_factory,
        dataset_factory=dataset_factory,
        workflow_factory=workflow_factory,
        client=client,
        tasks=fractal_tasks_mock_db,
        profile_id=profile.id,
    )


async def test_full_workflow_TaskExecutionError(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    override_settings_factory,
    fractal_tasks_mock_db,
    local_resource_profile_db,
):
    """ "
    Run a workflow made of three tasks, two successful tasks and one
    that raises an error.
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    resource, profile = local_resource_profile_db
    await full_workflow_TaskExecutionError(
        MockCurrentUser=MockCurrentUser,
        project_factory=project_factory,
        dataset_factory=dataset_factory,
        workflow_factory=workflow_factory,
        client=client,
        tasks=fractal_tasks_mock_db,
        profile_id=profile.id,
    )


async def test_non_executable_task_command_local(
    client,
    MockCurrentUser,
    testdata_path,
    task_factory,
    project_factory,
    dataset_factory,
    workflow_factory,
    override_settings_factory,
    local_resource_profile_db,
):
    """
    Execute a workflow with a task which has an invalid `command` (i.e. it is
    not executable).
    """
    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    resource, profile = local_resource_profile_db
    await non_executable_task_command(
        MockCurrentUser=MockCurrentUser,
        client=client,
        testdata_path=testdata_path,
        project_factory=project_factory,
        workflow_factory=workflow_factory,
        dataset_factory=dataset_factory,
        task_factory=task_factory,
        profile_id=profile.id,
    )


async def test_failing_workflow_UnknownError_local(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    monkeypatch,
    override_settings_factory,
    local_resource_profile_db,
):
    """
    Submit a workflow that fails with some unrecognized exception (due
    to a monkey-patched function in the runner).
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    resource, profile = local_resource_profile_db
    await failing_workflow_UnknownError(
        MockCurrentUser=MockCurrentUser,
        client=client,
        monkeypatch=monkeypatch,
        project_factory=project_factory,
        dataset_factory=dataset_factory,
        workflow_factory=workflow_factory,
        task_factory=task_factory,
        profile_id=profile.id,
    )


# Tested with 'local' backends only.
# With 'slurm' backend we get:
#   TaskExecutionError: bash: /home/runner/work/fractal-server/fractal-server/
#       tests/data/non_python_task_issue1377.sh: No such file or directory
async def test_non_python_task_local(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    testdata_path,
    tmp777_path,
    local_resource_profile_db,
):
    """
    Run a full workflow with a single bash task, which simply writes
    something to stderr and stdout
    """
    resource, profile = local_resource_profile_db

    await workflow_with_non_python_task(
        client=client,
        MockCurrentUser=MockCurrentUser,
        project_factory=project_factory,
        dataset_factory=dataset_factory,
        workflow_factory=workflow_factory,
        task_factory=task_factory,
        testdata_path=testdata_path,
        tmp777_path=tmp777_path,
        profile_id=profile.id,
    )


async def test_failing_workflow_post_task_execution(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    override_settings_factory,
    tmp_path,
    fractal_tasks_mock_db,
    local_resource_profile_db,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=FRACTAL_RUNNER_BACKEND)
    resource, profile = local_resource_profile_db

    await failing_workflow_post_task_execution(
        MockCurrentUser=MockCurrentUser,
        project_factory=project_factory,
        dataset_factory=dataset_factory,
        workflow_factory=workflow_factory,
        client=client,
        tasks=fractal_tasks_mock_db,
        tmp_path=tmp_path,
        profile_id=profile.id,
        resource_id=resource.id,
    )
