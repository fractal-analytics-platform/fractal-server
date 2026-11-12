import pytest

from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.ssh._fabric import FractalSSHList
from tests.v2._aux_slurm_rest import use_fake_slurm_rest
from tests.v2.test_07_full_workflow.common_functions import (
    workflow_with_non_python_task,
)


@pytest.mark.container
@pytest.mark.ssh
async def test_workflow_with_non_python_task_slurm_rest(
    client,
    app,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    slurm_rest_resource_profile_db,
    override_settings_factory,
    fractal_ssh,
    monkeypatch,
):
    """
    Same as `test_workflow_with_non_python_task_slurm_ssh`, with a
    `slurm_rest` resource: from the API to `SlurmRestRunner`, with
    a fake REST client that runs `sbatch` over SSH (no `slurmrestd` in the
    container).
    """
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.SLURM_REST)
    resource, profile = slurm_rest_resource_profile_db[:]
    fake_rest = use_fake_slurm_rest(monkeypatch, fractal_ssh)

    app.state.fractal_ssh_list = FractalSSHList()
    try:
        await workflow_with_non_python_task(
            MockCurrentUser=MockCurrentUser,
            profile_id=profile.id,
            project_dirs=[(tmp777_path / "project_dir").as_posix()],
            client=client,
            testdata_path=testdata_path,
            project_factory=project_factory,
            dataset_factory=dataset_factory,
            workflow_factory=workflow_factory,
            task_factory=task_factory,
            tmp777_path=tmp777_path,
        )
    finally:
        app.state.fractal_ssh_list.close_all()

    # The SLURM job went through the REST runner
    assert len(fake_rest.submitted) == 1
