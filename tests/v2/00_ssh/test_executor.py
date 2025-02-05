import json
import logging
from pathlib import Path

import pytest
from devtools import debug
from fabric.connection import Connection

from fractal_server.app.runner.executors.slurm.ssh.executor import (
    FractalSlurmSSHExecutor,
)  # noqa
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from tests.v2._aux_runner import (
    get_default_task_files,
)

logger = set_logger(__file__)


class MockFractalSlurmSSHExecutor(FractalSlurmSSHExecutor):
    """
    When running from outside Fractal runner, task-specific subfolders
    must be created by hand.
    """

    def _create_local_folder_structure(self):
        task_files = get_default_task_files(
            workflow_dir_local=self.workflow_dir_local,
            workflow_dir_remote=self.workflow_dir_remote,
        )
        local_subfolder = self.workflow_dir_local / task_files.subfolder_name
        logging.info(f"Now locally creating {local_subfolder.as_posix()}")
        local_subfolder.mkdir(parents=True)

    def _create_remote_folder_structure(self):
        task_files = get_default_task_files(
            workflow_dir_local=self.workflow_dir_local,
            workflow_dir_remote=self.workflow_dir_remote,
        )
        remote_subfolder = self.workflow_dir_remote / task_files.subfolder_name

        logging.info(f"Now remotely creating {remote_subfolder.as_posix()}")
        mkdir_command = f"mkdir -p {remote_subfolder.as_posix()}"
        stdout = self.fractal_ssh.run_command(cmd=mkdir_command)
        debug(stdout)
        logging.info(f"Now done creating {remote_subfolder.as_posix()}")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_local_folder_structure()
        self._create_remote_folder_structure()


def test_slurm_ssh_executor_handshake_fail(
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
    caplog,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    class MockFractalSSH(FractalSSH):
        def run_command(self, *args, **kwargs):
            return ""

        def check_connection(self):
            return True

    LOGGER_NAME = "invalid_ssh"
    with Connection(
        host="localhost",
        user="invalid",
        forward_agent=False,
        connect_kwargs={"password": "invalid"},
    ) as connection:
        mocked_fractal_ssh = MockFractalSSH(
            connection=connection,
            logger_name=LOGGER_NAME,
        )

        logger = logging.getLogger(LOGGER_NAME)
        logger.propagate = True

        with pytest.raises(json.decoder.JSONDecodeError):
            with MockFractalSlurmSSHExecutor(
                workflow_dir_local=tmp_path / "job_dir",
                workflow_dir_remote=(tmp777_path / "remote_job_dir"),
                slurm_poll_interval=1,
                fractal_ssh=mocked_fractal_ssh,
            ):
                log_text = caplog.text
                assert "Fractal server versions not available" in log_text
