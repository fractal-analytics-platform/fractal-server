import pytest
from devtools import debug
from fabric import Connection

from fractal_server.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHCommandError

from .aux_unit_runner import *  # noqa


@pytest.mark.ssh
@pytest.mark.container
async def test_run_squeue(
    tmp777_path,
    fractal_ssh: FractalSSH,
    slurm_ssh_resource_profile_db,
):
    fractal_ssh.default_lock_timeout = 1.0
    resource, profile = slurm_ssh_resource_profile_db[:]

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=tmp777_path / "server",
        root_dir_remote=tmp777_path / "user",
        user_cache_dir=(tmp777_path / "cache_dir").as_posix(),
        resource=resource,
        profile=profile,
    ) as runner:
        # Start a long SLURM job
        stdout = fractal_ssh.run_command(
            cmd="sbatch --parsable --wrap 'sleep 1000' "
        )
        debug(stdout)
        slurm_job_id = int(stdout.strip("\n"))

        # Case 1: invalid job IDs
        invalid_slurm_job_id = 99999999
        with pytest.raises(FractalSSHCommandError):
            runner.run_squeue(job_ids=[invalid_slurm_job_id])

        # Case 2: Empty list
        squeue_stdout = runner.run_squeue(job_ids=[])
        debug(squeue_stdout)
        assert squeue_stdout == ""

        # Case 3: one job is actually running
        squeue_stdout = runner.run_squeue(job_ids=[slurm_job_id])
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
            job_ids=[slurm_job_id],
        )
        debug(squeue_stdout)
        assert f"{slurm_job_id} FRACTAL_STATUS_PLACEHOLDER" in squeue_stdout

        # Release the lock
        fractal_ssh._lock.release()

        # Write `shutdown_file`, as an indirect way to stop `main_thread`
        runner.shutdown_file.touch()

        # Case 5: `FractalSSHConnectionError` results into empty stdout
        runner.fractal_ssh.close()
        with Connection("localhost") as connection:
            runner.fractal_ssh = FractalSSH(
                connection=connection,
            )
            squeue_stdout = runner.run_squeue(job_ids=[123])
            debug(squeue_stdout)
            assert squeue_stdout == ""
        runner.fractal_ssh.close()
