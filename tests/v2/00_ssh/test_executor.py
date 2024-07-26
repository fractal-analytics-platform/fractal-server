import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm.ssh.executor import (
    FractalSlurmSSHExecutor,
)  # noqa
from fractal_server.ssh._fabric import FractalSSH


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
        res = self.fractal_ssh.run(mkdir_command, hide=True)
        assert res.exited == 0
        logging.info(f"Now done creating {remote_subfolder.as_posix()}")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._create_local_folder_structure()
        self._create_remote_folder_structure()


def test_slurm_ssh_executor_submit(
    fractal_ssh,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/usr/bin/python{current_py_version}"
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


def test_slurm_ssh_executor_map(
    fractal_ssh: FractalSSH,
    tmp_path: Path,
    tmp777_path: Path,
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/usr/bin/python{current_py_version}"
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
        FRACTAL_SLURM_WORKER_PYTHON=f"/usr/bin/python{current_py_version}"
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
        FRACTAL_SLURM_WORKER_PYTHON=f"/usr/bin/python{current_py_version}"
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
            executor.wait_thread.wait(None)
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
