import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from devtools import debug
from fabric import Connection

from .aux_unit_runner import *  # noqa
from fractal_server.app.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHCommandError
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2.test_08_backends.aux_unit_runner import get_dummy_task_files


@pytest.mark.ssh
@pytest.mark.container
async def test_run_squeue(
    db,
    tmp777_path,
    history_mock_for_submit,
    fractal_ssh: FractalSSH,
    override_settings_factory,
    current_py_version: str,
):
    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON=f"/.venv{current_py_version}/bin/python{current_py_version}"  # noqa
    )

    fractal_ssh.default_lock_timeout = 1.0

    def sleep_long(parameters: dict, remote_files: dict):
        time.sleep(1_000)
        return 42

    history_run_id, history_unit_id = history_mock_for_submit

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        poll_interval=0,
    ) as runner:

        def main_thread():
            debug("[main_thread] START")
            result, exception = runner.submit(
                sleep_long,
                parameters=dict(zarr_urls=[]),
                task_files=get_dummy_task_files(
                    tmp777_path, component="", is_slurm=True
                ),
                task_type="non_parallel",
                history_unit_id=history_unit_id,
                config=get_default_slurm_config(),
            )
            debug("[main_thread] END")
            return result, exception

        # Case 1: invalid job IDs
        invalid_slurm_job_id = 99999999
        with pytest.raises(FractalSSHCommandError):
            runner.run_squeue(job_ids=[invalid_slurm_job_id])

        # Case 2: `runner.jobs = {}`
        squeue_stdout = runner.run_squeue(job_ids=runner.job_ids)
        debug(squeue_stdout)
        assert "PENDING" not in squeue_stdout
        assert "RUNNING" not in squeue_stdout

        with ThreadPoolExecutor(max_workers=10) as executor:

            # Submit a `sleep_long` function
            fut_main = executor.submit(main_thread)

            # Wait a bit, until the job is submitted
            while runner.jobs == {}:
                time.sleep(0.05)
            slurm_job_id = runner.job_ids[0]
            debug(slurm_job_id)

            # Case 3: one job is actually running
            squeue_stdout = runner.run_squeue(job_ids=runner.job_ids)
            debug(squeue_stdout)
            assert f"{slurm_job_id} " in squeue_stdout
            PENDING_MSG = f"{slurm_job_id} PENDING"
            RUNNING_MSG = f"{slurm_job_id} RUNNING"
            assert PENDING_MSG in squeue_stdout or RUNNING_MSG in squeue_stdout

            # Acquire and keep the `FractalSSH` lock
            fractal_ssh._lock.acquire(timeout=4.0)

            # Case 4: When `FractalSSH` lock cannot be acquired, a placeholder
            # must be returned
            squeue_stdout = runner.run_squeue(
                job_ids=runner.job_ids,
                max_attempts=1,
            )
            debug(squeue_stdout)
            assert (
                f"{slurm_job_id} FRACTAL_STATUS_PLACEHOLDER" in squeue_stdout
            )

            # Release the lock
            fractal_ssh._lock.release()

            # Write `shutdown_file`, as an indirect way to stop `main_thread`
            runner.shutdown_file.touch()
            main_result = fut_main.result()
            debug(main_result)

        # Case 5: `FractalSSHConnectionError` results into empty stdout
        with Connection("localhost") as connection:
            runner.fractal_ssh = FractalSSH(
                connection=connection,
                default_base_interval=1.0,
            )
            squeue_stdout = runner.run_squeue(job_ids=[123], max_attempts=1)
            debug(squeue_stdout)
            assert squeue_stdout == ""
