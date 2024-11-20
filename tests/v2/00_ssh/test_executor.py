import json
import logging
import threading
import time
from pathlib import Path

import pytest
from devtools import debug
from fabric.connection import Connection

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm.ssh.executor import (
    FractalSlurmSSHExecutor,
)  # noqa
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH

logger = set_logger(__file__)


class MockFractalSSHSlurmExecutor(FractalSlurmSSHExecutor):
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
        mkdir_command = f"mkdir -p {remote_subfolder.as_posix()}"
        stdout = self.fractal_ssh.run_command(cmd=mkdir_command)
        debug(stdout)
        logging.info(f"Now done creating {remote_subfolder.as_posix()}")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_local_folder_structure()
        self._create_remote_folder_structure()


def test_errors_failed_init_1(
    override_settings_factory,
    fractal_ssh,
    tmp_path,
    tmp777_path,
):
    """
    Check that an exception in `FractalSSHSlurmExecutor.__init__`
    is handled correctly.
    """
    threads_pre = threading.enumerate()
    override_settings_factory(FRACTAL_SLURM_WORKER_PYTHON=None)
    with pytest.raises(
        ValueError,
        match="FRACTAL_SLURM_WORKER_PYTHON is not set",
    ):
        with MockFractalSSHSlurmExecutor(
            workflow_dir_local=tmp_path / "job_dir",
            workflow_dir_remote=(tmp777_path / "remote_job_dir"),
            slurm_poll_interval=1,
            fractal_ssh=fractal_ssh,
        ):
            pass
    threads_post = threading.enumerate()
    debug(threads_pre, threads_post)
    assert len(threads_post) == len(threads_pre)


def test_errors_failed_init_2(
    override_settings_factory,
    current_py_version,
    fractal_ssh,
    tmp_path,
    tmp777_path,
):
    """
    Check that an exception in `FractalSSHSlurmExecutor.__init__`
    is handled correctly.
    """
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )
    threads_pre = threading.enumerate()
    with pytest.raises(RuntimeError, match="SLURM account must be set"):
        with MockFractalSSHSlurmExecutor(
            workflow_dir_local=tmp_path / "job_dir",
            workflow_dir_remote=(tmp777_path / "remote_job_dir"),
            slurm_poll_interval=1,
            fractal_ssh=fractal_ssh,
            common_script_lines=["#SBATCH --account=something"],
        ):
            pass
    threads_post = threading.enumerate()
    assert len(threads_post) == len(threads_pre)


def test_errors_failed_init_3(
    override_settings_factory,
    current_py_version,
    fractal_ssh_list,
    tmp_path,
    tmp777_path,
    slurmlogin_ip,
):
    """
    Check that an exception in `FractalSSHSlurmExecutor.__init__`
    is handled correctly.
    """
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    key_path = (tmp_path / "my.key").as_posix()
    invalid_fractal_obj = fractal_ssh_list.get(
        host="invalid_host",
        user="invalid_user",
        key_path=key_path,
    )
    threads_pre = threading.enumerate()
    with pytest.raises(
        RuntimeError,
        match="Cannot open SSH connection",
    ) as exc_info:
        with MockFractalSSHSlurmExecutor(
            workflow_dir_local=tmp_path / "job_dir",
            workflow_dir_remote=(tmp777_path / "remote_job_dir"),
            slurm_poll_interval=1,
            fractal_ssh=invalid_fractal_obj,
        ):
            pass
    debug(exc_info.value)
    threads_post = threading.enumerate()
    assert len(threads_post) == len(threads_pre)


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
            with MockFractalSSHSlurmExecutor(
                workflow_dir_local=tmp_path / "job_dir",
                workflow_dir_remote=(tmp777_path / "remote_job_dir"),
                slurm_poll_interval=1,
                fractal_ssh=mocked_fractal_ssh,
            ):

                log_text = caplog.text
                assert "Fractal server versions not available" in log_text


def test_slurm_ssh_executor_submit(
    fractal_ssh,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=(tmp777_path / "remote_job_dir"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        fut = executor.submit(lambda: 1)
        debug(fut)
        debug(fut.result())

    # Assert that no .tar.gz is left in the job directory, see
    # https://github.com/fractal-analytics-platform/fractal-server/issues/1715
    assert len(list((tmp_path / "job_dir").glob("*"))) > 0
    assert len(list((tmp_path / "job_dir").glob("*.tar.gz"))) == 0


def test_slurm_ssh_executor_map(
    fractal_ssh: FractalSSH,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=(tmp777_path / "remote_job_dir"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        res = executor.map(lambda x: x * 2, [1, 2, 3])
        results = list(res)
        assert results == [2, 4, 6]


def test_slurm_ssh_executor_submit_with_pre_sbatch(
    fractal_ssh,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )
    from fractal_server.app.runner.executors.slurm._slurm_config import (
        get_default_slurm_config,
    )

    auxfile = tmp777_path / "auxfile"
    slurm_config = get_default_slurm_config()
    slurm_config.pre_submission_commands = [f"touch {auxfile.as_posix()}"]
    debug(slurm_config)

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=(tmp777_path / "remote_job_dir"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        fut = executor.submit(lambda: 1, slurm_config=slurm_config)
        debug(fut)
        debug(fut.result())

    assert auxfile.exists()


def test_slurm_ssh_executor_shutdown_before_job_submission(
    fractal_ssh,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    """
    Verify the behavior when shutdown is called before any job has started.
    """

    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir1",
        workflow_dir_remote=(tmp777_path / "remote_job_dir1"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            fut = executor.submit(lambda: 1)
            fut.result()
        debug(exc_info.value)

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir2",
        workflow_dir_remote=(tmp777_path / "remote_job_dir2"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            fut = executor.map(lambda x: 1, [1, 2, 3])
            fut.result()
        debug(exc_info.value)

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir3",
        workflow_dir_remote=(tmp777_path / "remote_job_dir3"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            executor.wait_thread.wait(job_id=1)
        debug(exc_info.value)

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir4",
        workflow_dir_remote=(tmp777_path / "remote_job_dir4"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        executor.shutdown()
        with pytest.raises(JobExecutionError) as exc_info:
            executor._submit_job(None)
        debug(exc_info.value)


def test_slurm_ssh_executor_error_in_calllback(
    fractal_ssh,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
    monkeypatch,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=(
            f"/.venv{current_py_version}/bin/python{current_py_version}"
        )
    )

    def _get_subfolder_sftp_patched(*args, **kwargs):
        debug("NOW RUNNING _get_subfolder_sftp_patched")
        raise RuntimeError(
            "This is an error from `_get_subfolder_sftp_patched`"
        )

    monkeypatch.setattr(
        MockFractalSSHSlurmExecutor,
        "_get_subfolder_sftp",
        _get_subfolder_sftp_patched,
    )

    with MockFractalSSHSlurmExecutor(
        workflow_dir_local=tmp_path / "job_dir",
        workflow_dir_remote=(tmp777_path / "remote_job_dir"),
        slurm_poll_interval=1,
        fractal_ssh=fractal_ssh,
    ) as executor:
        fut = executor.submit(lambda: 1)
        debug(fut)

        TIMEOUT = 5
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < TIMEOUT:
            time.sleep(0.4)
            debug(fut._state)
            if fut._state != "PENDING":
                return

        raise RuntimeError(f"Future still pending, {fut}")
