import pytest

from fractal_server.ssh._fabric import FractalSSHList
from tests.fixtures_slurm import SLURM_USER
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
    ssh_alive,
    slurmlogin_ip,
    monkeypatch,
    ssh_keys: dict[str, str],
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        ),
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path=ssh_keys["private"],
        ssh_tasks_dir=(tmp777_path / "tasks").as_posix(),
        ssh_jobs_dir=(tmp777_path / "artifacts").as_posix(),
    )

    app.state.fractal_ssh_list = FractalSSHList()

    await workflow_with_non_python_task(
        MockCurrentUser=MockCurrentUser,
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
    ssh_alive,
    slurmlogin_ip,
    monkeypatch,
    override_settings_factory,
    current_py_version: str,
):
    """
    Setup faulty SSH connection (with wrong path to key file) and observe
    first failure point.
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_SLURM_WORKER_PYTHON=f"/usr/bin/python{current_py_version}",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    user_settings_dict = dict(
        ssh_host=slurmlogin_ip,
        ssh_username=SLURM_USER,
        ssh_private_key_path="/invalid/path",
        ssh_tasks_dir=(tmp777_path / "tasks").as_posix(),
        ssh_jobs_dir=(tmp777_path / "artifacts").as_posix(),
    )

    app.state.fractal_ssh_list = FractalSSHList()

    job_logs = await workflow_with_non_python_task(
        MockCurrentUser=MockCurrentUser,
        client=client,
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
