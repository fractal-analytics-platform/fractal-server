import io
import json
import logging
import random
from pathlib import Path

from devtools import debug  # noqa: F401
from fabric.connection import Connection

from fractal_server.app.runner.executors.slurm_ssh.executor import (
    FractalSlurmSSHExecutor,
)  # noqa


def test_versions(
    slurmlogin_ip,
    ssh_alive,
    slurmlogin_container,
    monkeypatch,
    ssh_keys: dict[str, str],
):
    """
    Check the Python and fractal-server versions available on the cluster.
    NOTE: This will later become a preliminary-check as part of the app
    startup phase: check that Python has the same Major.Minor versions
    and fractal-server has the same Major.Minor.Patch.
    """
    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        connect_kwargs={"password": "fractal"},
    ) as connection:

        command = "/usr/bin/python3.9 --version"
        print(f"COMMAND:\n{command}")
        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")

        python_command = "import fractal_server as fs; print(fs.__VERSION__);"
        command = f"/usr/bin/python3.9 -c '{python_command}'"

        print(f"COMMAND:\n{command}")
        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")

    print("NOW AGAIN BUT USING KEY")
    ssh_private_key = ssh_keys["private"]
    debug(ssh_private_key)
    debug(slurmlogin_ip)

    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        connect_kwargs={"key_filename": ssh_private_key},
    ) as connection:

        command = "/usr/bin/python3.9 --version"
        print(f"COMMAND:\n{command}")
        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")

        python_command = "import fractal_server as fs; print(fs.__VERSION__);"
        command = f"/usr/bin/python3.9 -c '{python_command}'"

        print(f"COMMAND:\n{command}")
        res = connection.run(command, hide=True)
        print(f"STDOUT:\n{res.stdout}")
        print(f"STDERR:\n{res.stderr}")

    # -o "StrictHostKeyChecking no"


class TestingFractalSSHSlurmExecutor(FractalSlurmSSHExecutor):
    """
    When running from outside Fractal runner, task-specific subfolders
    must be created by hand.
    """

    def _create_local_folder_structure(self):
        task_files = self.get_default_task_files()
        local_subfolder = self.workflow_dir_local / task_files.subfolder_name
        logging.info(f"Now locally creating {local_subfolder.as_posix()}")
        local_subfolder.mkdir(parents=True)

    def _create_remote_folder_structure(self):
        task_files = self.get_default_task_files()
        remote_subfolder = self.workflow_dir_remote / task_files.subfolder_name

        logging.info(f"Now remotely creating {remote_subfolder.as_posix()}")
        with self.ConnectionWithParameters() as connection:
            mkdir_command = f"mkdir -p {remote_subfolder.as_posix()}"
            res = connection.run(mkdir_command, hide=True)
            assert res.exited == 0
        logging.info(f"Now done creating {remote_subfolder.as_posix()}")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_local_folder_structure()
        self._create_remote_folder_structure()


def test_slurm_ssh_executor_submit(
    slurmlogin_ip,
    ssh_alive,
    monkeypatch,
    tmp_path: Path,
    tmp777_path: Path,
    ssh_keys: dict[str, str],
    override_settings_factory,
):
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON="/usr/bin/python3.9")

    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with TestingFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=(tmp777_path / "remote_job_dir"),
        slurm_poll_interval=1,
        ssh_host=slurmlogin_ip,
        ssh_user="test01",
        ssh_private_key_path=ssh_keys["private"],
    ) as executor:
        fut = executor.submit(lambda: 1)
        debug(fut)
        debug(fut.result())


def test_slurm_ssh_executor_map(
    slurmlogin_ip,
    ssh_alive,
    monkeypatch,
    tmp_path: Path,
    tmp777_path: Path,
    ssh_keys: dict[str, str],
    override_settings_factory,
):
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON="/usr/bin/python3.9")

    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with TestingFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=(tmp777_path / "remote_job_dir"),
        slurm_poll_interval=0,
        ssh_host=slurmlogin_ip,
        ssh_user="test01",
        ssh_private_key_path=ssh_keys["private"],
    ) as executor:
        res = executor.map(lambda x: x * 2, [1, 2, 3])
        results = list(res)
        assert results == [2, 4, 6]


def test_slurm_ssh_executor_no_docker(
    monkeypatch,
    tmp_path,
    testdata_path,
    override_settings_factory,
):
    """
    This test requires a configuration file pointing to a SLURM cluster
    that can be reached via SSH.
    """

    ssh_config_file = testdata_path / "ssh_config"
    if not ssh_config_file.exists():
        logging.warning(f"Missing {ssh_config_file} -- skip test.")
        return

    random.seed(tmp_path.as_posix())
    label = str(random.randrange(0, 999999))

    monkeypatch.setattr("sys.stdin", io.StringIO(""))

    with ssh_config_file.open("r") as f:
        config = json.load(f)["uzh1"]
    debug(config)

    remote_python = config.pop("remote_python")
    root_dir_remote = Path(config.pop("root_dir_remote"))
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=remote_python,
    )
    from fractal_server.app.runner.executors.slurm._slurm_config import (
        get_default_slurm_config,
    )

    slurm_config = get_default_slurm_config()
    slurm_config.partition = config.pop("partition")
    slurm_config.mem_per_task_MB = config.pop("mem_per_task_MB")

    debug(slurm_config)

    with TestingFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / f"local_job_dir_{label}",
        workflow_dir_remote=root_dir_remote / f"remote_job_dir_{label}",
        slurm_poll_interval=1,
        **config,
    ) as executor:
        fut = executor.submit(lambda: 1, slurm_config=slurm_config)
        debug(fut)
        debug(fut.result())
