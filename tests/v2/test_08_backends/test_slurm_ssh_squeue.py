import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from devtools import debug

from .aux_unit_runner import *  # noqa
from .aux_unit_runner import ZARR_URLS
from fractal_server.app.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.ssh._fabric import _acquire_lock_with_timeout
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
                parameters=dict(zarr_urls=ZARR_URLS),
                task_files=get_dummy_task_files(
                    tmp777_path, component="", is_slurm=True
                ),
                task_type="non_parallel",
                history_unit_id=history_unit_id,
                config=get_default_slurm_config(),
            )
            debug("[main_thread] END")
            return result, exception

        def squeue_thread():
            try:
                debug("[squeue_thread] START")
                stdout = runner.run_squeue(
                    job_ids=runner.job_ids,
                    max_attempts=1,
                )
                debug("[squeue_thread] END")
                return stdout
            except Exception as e:
                debug("[squeue_thread]", e)
                return e

        def keep_lock_thread():
            try:
                debug("[keep_lock_thread] START")
                with _acquire_lock_with_timeout(
                    lock=runner.fractal_ssh._lock,
                    label="keep_lock_thread",
                    timeout=4.0,
                ):
                    debug("[keep_lock_thread] LOCK ACQUIRED, NOW SLEEP..")
                    while True:
                        if runner.shutdown_file.exists():
                            debug("[keep_lock_thread] END")
                            return None
                        else:
                            debug("[keep_lock_thread] Sleep and continue.")
                            time.sleep(0.1)
            except Exception as e:
                debug("[keep_lock_thread]", e)
                return e

        # Case 1: invalid job IDs
        invalid_slurm_job_id = 99999999
        with pytest.raises(FractalSSHCommandError):
            runner.run_squeue(job_ids=[invalid_slurm_job_id])

        with ThreadPoolExecutor(max_workers=10) as executor:

            # Case 2: `runner.jobs = {}`
            fut_squeue = executor.submit(squeue_thread)
            squeue_stdout = fut_squeue.result()
            debug(squeue_stdout)
            assert "PENDING" not in squeue_stdout
            assert "RUNNING" not in squeue_stdout

            # Submit a `sleep_long` function
            fut_main = executor.submit(main_thread)

            # Wait a bit, to make sure the job was submitted
            time.sleep(0.5)

            # Case 3: one job is actually running
            fut_squeue = executor.submit(squeue_thread)
            squeue_stdout = fut_squeue.result()
            debug(squeue_stdout)
            assert "PENDING" in squeue_stdout or "RUNNING" in squeue_stdout

            # Case 4: FractalSSH lock cannot be acquired
            fut_lock = executor.submit(keep_lock_thread)
            fut_squeue = executor.submit(squeue_thread)
            squeue_stdout = fut_squeue.result()
            debug(squeue_stdout)
            assert "FRACTAL_STATUS_PLACEHOLDER" in squeue_stdout

            # Write `shutdown_file`, as an indirect way to stop both
            # `main_thread` and `keep_lock_thread`
            runner.shutdown_file.touch()

            keep_lock_result = fut_lock.result()
            debug(keep_lock_result)

            assert keep_lock_result is None
            main_result = fut_main.result()
            debug(main_result)
