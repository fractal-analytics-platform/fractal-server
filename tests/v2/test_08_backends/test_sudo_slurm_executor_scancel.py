import os
import shlex
import subprocess
import time
from pathlib import Path

import pytest

from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.slurm.sudo._subprocess_run_as_user import (  # noqa: E501
    _mkdir_as_user,
)
from fractal_server.app.runner.executors.slurm.sudo.executor import (
    FractalSlurmSudoExecutor,
)
from tests.fixtures_slurm import run_squeue
from tests.fixtures_slurm import SLURM_USER
from tests.v2._aux_runner import get_default_slurm_config
from tests.v2._aux_runner import get_default_task_files


@pytest.mark.container
async def test_scancel_during_execution(
    tmp777_path: Path, monkey_slurm, slurm_working_folders
):
    """
    Test the scenario where `scancel` is called during a
    `FractalSlurmExecutor` execution of `.submit` or `.map`.
    """
    # Define and create folders and subfolders for both cases
    base_dir_local, base_dir_remote = slurm_working_folders
    job_folders = {}
    for job_name in ["job1", "job2"]:
        job_dir_local = base_dir_local / job_name
        job_dir_remote = base_dir_remote / job_name
        job_folders[job_name] = dict(
            local=job_dir_local,
            remote=job_dir_remote,
        )
        task_files = get_default_task_files(
            workflow_dir_local=job_dir_local,
            workflow_dir_remote=job_dir_remote,
        )
        umask = os.umask(0)
        (job_dir_local / task_files.subfolder_name).mkdir(parents=True)
        os.umask(umask)
        _mkdir_as_user(
            folder=str(job_dir_remote / task_files.subfolder_name),
            user=SLURM_USER,
        )

    scancel_cmd = (
        f"sudo --non-interactive -u {SLURM_USER} scancel -u {SLURM_USER} -v"
    )

    # JOB 1: fail during `submit`
    with pytest.raises(JobExecutionError) as exc_info:
        with FractalSlurmSudoExecutor(
            workflow_dir_local=job_folders["job1"]["local"],
            workflow_dir_remote=job_folders["job1"]["remote"],
            slurm_user=SLURM_USER,
            slurm_poll_interval=1,
        ) as executor:
            # Submit task
            fut = executor.submit(
                time.sleep,
                100,
                slurm_config=get_default_slurm_config(),
                task_files=task_files,
            )
            # Wait and then scancel
            while "RUNNING" not in run_squeue(squeue_format="%i %T"):
                time.sleep(0.1)
            subprocess.run(
                shlex.split(scancel_cmd), capture_output=True, encoding="utf-8"
            )
            # Trigger exception
            fut.result()
    job_execution_error = exc_info.value
    assert "CANCELLED" in job_execution_error.assemble_error()

    # JOB 2: fail during `map`
    with pytest.raises(JobExecutionError) as exc_info:
        with FractalSlurmSudoExecutor(
            workflow_dir_local=job_folders["job2"]["local"],
            workflow_dir_remote=job_folders["job2"]["remote"],
            slurm_user=SLURM_USER,
            slurm_poll_interval=1,
        ) as executor:
            # Submit task
            res = executor.map(
                time.sleep,
                [100, 101],
                slurm_config=get_default_slurm_config(),
                task_files=task_files,
            )
            # Wait and then scancel
            while "RUNNING" not in run_squeue(squeue_format="%i %T"):
                time.sleep(0.1)
            subprocess.run(
                shlex.split(scancel_cmd), capture_output=True, encoding="utf-8"
            )
            # Trigger exception
            list(res)
    job_execution_error = exc_info.value
    assert "CANCELLED" in job_execution_error.assemble_error()
