import pytest

from fractal_server.ssh._fabric import FractalSSHList
from tests.v2.test_07_full_workflow.common_functions import (
    workflow_with_non_python_task,
)


FRACTAL_RUNNER_BACKEND = "slurm_ssh"


@pytest.mark.container
@pytest.mark.ssh
async def test_workflow_with_non_python_task_slurm_ssh(
    client,
    app,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    slurm_ssh_resource_profile_db,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm_ssh")
    resource, profile = slurm_ssh_resource_profile_db[:]

    user_settings_dict = dict(
        ssh_host=resource.host,
        ssh_username=profile.username,
        ssh_private_key_path=profile.ssh_key_path,
        ssh_tasks_dir=(tmp777_path / "tasks").as_posix(),
        ssh_jobs_dir=(tmp777_path / "artifacts").as_posix(),
    )

    app.state.fractal_ssh_list = FractalSSHList()

    await workflow_with_non_python_task(
        MockCurrentUser=MockCurrentUser,
        additional_user_kwargs=dict(profile_id=profile.id),
        user_settings_dict=user_settings_dict,
        client=client,
        testdata_path=testdata_path,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
        tmp777_path=tmp777_path,
    )

    app.state.fractal_ssh_list.close_all()


@pytest.mark.container
@pytest.mark.ssh
async def test_workflow_with_non_python_task_slurm_ssh_fail(
    client,
    app,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    slurm_ssh_resource_profile_db,
    override_settings_factory,
):
    """
    Setup faulty SSH connection (with wrong path to key file) and observe
    first failure point.
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm_ssh")
    resource, profile = slurm_ssh_resource_profile_db[:]
    profile.ssh_key_path = "/invalid/path"

    user_settings_dict = dict(
        ssh_host=resource.host,
        ssh_username=profile.username,
        ssh_private_key_path=profile.ssh_key_path,
        ssh_tasks_dir=(tmp777_path / "tasks").as_posix(),
        ssh_jobs_dir=(tmp777_path / "artifacts").as_posix(),
    )

    app.state.fractal_ssh_list = FractalSSHList()

    job_logs = await workflow_with_non_python_task(
        MockCurrentUser=MockCurrentUser,
        client=client,
        additional_user_kwargs=dict(profile_id=profile.id),
        user_settings_dict=user_settings_dict,
        testdata_path=testdata_path,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
        tmp777_path=tmp777_path,
        this_should_fail=True,
    )
    assert (
        "No such file or directory: "
        f"'{user_settings_dict['ssh_private_key_path']}'" in job_logs
    )

    app.state.fractal_ssh_list.close_all()
