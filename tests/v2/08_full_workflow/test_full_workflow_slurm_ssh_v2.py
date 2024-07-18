import io

from common_functions import workflow_with_non_python_task

from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import get_ssh_connection
from tests.fixtures_slurm import SLURM_USER


FRACTAL_RUNNER_BACKEND = "slurm_ssh"


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
        FRACTAL_SLURM_WORKER_PYTHON=f"/usr/bin/python{current_py_version}",
        FRACTAL_SLURM_SSH_HOST=slurmlogin_ip,
        FRACTAL_SLURM_SSH_USER=SLURM_USER,
        FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH=ssh_keys["private"],
        FRACTAL_SLURM_SSH_WORKING_BASE_DIR=(
            tmp777_path / "artifacts"
        ).as_posix(),
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    connection = get_ssh_connection()
    app.state.fractal_ssh = FractalSSH(connection=connection)

    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    await workflow_with_non_python_task(
        MockCurrentUser=MockCurrentUser,
        client=client,
        testdata_path=testdata_path,
        project_factory_v2=project_factory_v2,
        dataset_factory_v2=dataset_factory_v2,
        workflow_factory_v2=workflow_factory_v2,
        task_factory_v2=task_factory_v2,
        tmp777_path=tmp777_path,
    )
