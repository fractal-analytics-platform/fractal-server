import time
from pathlib import Path
from typing import Self
from typing import override

from paramiko.ssh_exception import NoValidConnectionsError
from paramiko.ssh_exception import SSHException

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.logger import set_logger
from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.runner.executors.slurm_common.base_slurm_runner import (
    BaseSlurmRunner,
)
from fractal_server.runner.executors.slurm_common.slurm_job_task_models import (
    SlurmJob,
)
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHCommandError
from fractal_server.ssh._fabric import FractalSSHTimeoutError
from fractal_server.ssh._fabric import FractalSSHUnknownError

from .run_subprocess import run_subprocess
from .tar_commands import get_tar_compression_cmd
from .tar_commands import get_tar_extraction_cmd

logger = set_logger(__name__)

# `run_squeue` placeholder returned when we cannot actually query SLURM
# (SSH transport failure, lock timeout). Chosen so that it is NOT part of
# `STATES_FINISHED`, which prevents `_get_finished_jobs` from wrongly
# marking every polled job as COMPLETED when the SSH channel is unusable.
_SQUEUE_STATUS_PLACEHOLDER = "FRACTAL_STATUS_PLACEHOLDER"

# Transport-level SSH exceptions that indicate the channel is (possibly)
# broken but not that any actual command failed. When we see these we
# should NOT interpret them as "no jobs / task done" and we should retry
# the operation later (after refreshing the connection).
_SSH_TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (
    OSError,
    EOFError,
    SSHException,
    NoValidConnectionsError,
    FractalSSHUnknownError,
    FractalSSHTimeoutError,
)


class SlurmSSHRunner(BaseSlurmRunner):
    """
    Runner implementation for a computational `slurm_ssh` resource.
    """

    fractal_ssh: FractalSSH

    def __init__(
        self,
        *,
        # Common
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: list[str] | None = None,
        resource: Resource,
        # Specific
        slurm_account: str | None = None,
        profile: Profile,
        user_cache_dir: str,
        fractal_ssh: FractalSSH,
        fractal_job_id: int,
        resource_id: int,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """
        self.fractal_ssh = fractal_ssh
        self.shared_config = JobRunnerConfigSLURM(**resource.jobs_runner_config)
        logger.warning(self.fractal_ssh)

        # Check SSH connection and try to recover from a closed-socket error
        self.fractal_ssh.check_connection()
        super().__init__(
            slurm_runner_type="ssh",
            root_dir_local=root_dir_local,
            root_dir_remote=root_dir_remote,
            common_script_lines=common_script_lines,
            user_cache_dir=user_cache_dir,
            poll_interval=resource.jobs_poll_interval,
            python_worker_interpreter=resource.jobs_slurm_python_worker,
            slurm_account=slurm_account,
            fractal_job_id=fractal_job_id,
            resource_id=resource_id,
        )

    @override
    def _mkdir_local_folder(self: Self, folder: str) -> None:
        Path(folder).mkdir(parents=True)

    @override
    def _mkdir_remote_folder(self: Self, folder: str) -> None:
        self.fractal_ssh.mkdir(
            folder=folder,
            parents=True,
        )

    def _fetch_artifacts(
        self,
        finished_slurm_jobs: list[SlurmJob],
    ) -> None:
        """
        Fetch artifacts for a list of SLURM jobs, with retries on SSH failure.

        Wraps `_fetch_artifacts_impl` with exponential backoff and an SSH
        reconnection attempt between retries. On final failure, re-raises
        the last exception (callers mark the affected tasks as FAILED).
        """
        if len(finished_slurm_jobs) == 0:
            logger.debug(f"[_fetch_artifacts] EXIT ({finished_slurm_jobs=}).")
            return None

        max_attempts = 5
        base_wait_seconds = 2.0
        last_exception: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                self._fetch_artifacts_impl(finished_slurm_jobs)
                if attempt > 1:
                    logger.warning(
                        f"[_fetch_artifacts] Succeeded on attempt {attempt}/"
                        f"{max_attempts} after transient failure(s)."
                    )
                return None
            except Exception as e:
                last_exception = e
                logger.warning(
                    f"[_fetch_artifacts] Attempt {attempt}/{max_attempts} "
                    f"failed ({type(e).__name__}): {e}"
                )
                if attempt == max_attempts:
                    break
                # Try to bring the SSH channel back before the next attempt.
                try:
                    self.fractal_ssh.check_connection()
                except Exception as reconnect_err:
                    logger.warning(
                        "[_fetch_artifacts] SSH reconnection attempt failed: "
                        f"{reconnect_err}. Will still retry the fetch."
                    )
                wait_seconds = base_wait_seconds * (2 ** (attempt - 1))
                logger.info(
                    f"[_fetch_artifacts] Sleeping {wait_seconds:.1f}s before "
                    f"retry {attempt + 1}/{max_attempts}."
                )
                time.sleep(wait_seconds)

        logger.error(
            f"[_fetch_artifacts] Giving up after {max_attempts} attempts. "
            f"Last error ({type(last_exception).__name__}): {last_exception}. "
            "Affected SLURM jobs will be marked as FAILED even though the "
            "compute may have completed successfully on the cluster. Task "
            "output files can still be found on the remote working directory."
        )
        raise last_exception  # type: ignore[misc]

    def _fetch_artifacts_impl(
        self,
        finished_slurm_jobs: list[SlurmJob],
    ) -> None:
        """
        Fetch artifacts for a list of SLURM jobs.

        Note: this is idempotent -- the remote tarball, the local tarball
        and the extracted files are all overwritten on each call. The
        temporary remote filelist gets a unique name (`time.time()` suffix)
        so leftovers from a failed attempt do not collide. This makes the
        retry-wrapper `_fetch_artifacts` safe to invoke multiple times.
        """

        t_0 = time.perf_counter()
        logger.debug(f"[_fetch_artifacts] START ({len(finished_slurm_jobs)=}).")

        # Extract `workdir_remote` and `workdir_local`
        self.validate_slurm_jobs_workdirs(finished_slurm_jobs)
        workdir_local = finished_slurm_jobs[0].workdir_local
        workdir_remote = finished_slurm_jobs[0].workdir_remote

        # Define local/remote tarfile paths
        tarfile_path_local = workdir_local.with_suffix(".tar.gz").as_posix()
        tarfile_path_remote = workdir_remote.with_suffix(".tar.gz").as_posix()

        # Create file list
        # NOTE: see issue 2483
        filelist = []
        for _slurm_job in finished_slurm_jobs:
            _single_job_filelist = [
                _slurm_job.slurm_stdout_remote_path.name,
                _slurm_job.slurm_stderr_remote_path.name,
            ]
            for task in _slurm_job.tasks:
                _single_job_filelist.extend(
                    [
                        task.output_file_remote_path.name,
                        task.task_files.log_file_remote_path.name,
                        task.task_files.metadiff_file_remote_path.name,
                    ]
                )
            filelist.extend(_single_job_filelist)
        filelist_string = "\n".join(filelist)
        elapsed = time.perf_counter() - t_0
        logger.debug(
            "[_fetch_artifacts] Created filelist "
            f"({len(filelist)=}, from start: {elapsed=:.3f} s)."
        )

        # Write filelist to file remotely
        tmp_filelist_path = workdir_remote / f"filelist_{time.time()}.txt"
        self.fractal_ssh.write_remote_file(
            path=tmp_filelist_path.as_posix(),
            content=f"{filelist_string}\n",
        )
        elapsed = time.perf_counter() - t_0
        logger.debug(
            f"[_fetch_artifacts] File list written to {tmp_filelist_path} "
            f"(from start: {elapsed=:.3f} s)."
        )

        # Create remote tarfile
        t_0_tar = time.perf_counter()
        tar_command = get_tar_compression_cmd(
            subfolder_path=workdir_remote,
            filelist_path=tmp_filelist_path,
        )
        self.fractal_ssh.run_command(cmd=tar_command)
        t_1_tar = time.perf_counter()
        logger.info(
            f"[_fetch_artifacts] Remote archive {tarfile_path_remote} created"
            f" - elapsed={t_1_tar - t_0_tar:.3f} s"
        )

        # Fetch tarfile
        t_0_get = time.perf_counter()
        self.fractal_ssh.fetch_file(
            remote=tarfile_path_remote,
            local=tarfile_path_local,
        )
        t_1_get = time.perf_counter()
        logger.info(
            "[_fetch_artifacts] Subfolder archive transferred back "
            f"to {tarfile_path_local}"
            f" - elapsed={t_1_get - t_0_get:.3f} s"
        )

        # Extract tarfile locally
        target_dir, cmd_tar = get_tar_extraction_cmd(Path(tarfile_path_local))
        target_dir.mkdir(exist_ok=True)
        run_subprocess(cmd=cmd_tar, logger_name=logger.name)
        Path(tarfile_path_local).unlink(missing_ok=True)

        t_1 = time.perf_counter()
        logger.info(f"[_fetch_artifacts] End - elapsed={t_1 - t_0:.3f} s")

    @override
    def _run_remote_cmd(self: Self, cmd: str) -> str:
        stdout = self.fractal_ssh.run_command(cmd=cmd)
        return stdout

    def _send_many_job_inputs(
        self, *, workdir_local: Path, workdir_remote: Path
    ) -> None:
        """
        Compress, transfer, and extract a local working directory onto a remote
        host.

        This method creates a temporary `.tar.gz` archive of the given
        `workdir_local`, transfers it to the remote machine via the configured
        SSH connection, extracts it into `workdir_remote`, and removes the
        temporary archive from both local and remote filesystems.
        """

        logger.debug("[_send_many_job_inputs] START")

        tar_path_local = workdir_local.with_suffix(".tar.gz")
        tar_name = Path(tar_path_local).name
        tar_path_remote = workdir_remote.parent / tar_name

        tar_compression_cmd = get_tar_compression_cmd(
            subfolder_path=workdir_local, filelist_path=None
        )
        _, tar_extraction_cmd = get_tar_extraction_cmd(
            archive_path=tar_path_remote
        )
        rm_tar_cmd = f"rm {tar_path_remote.as_posix()}"

        try:
            run_subprocess(tar_compression_cmd, logger_name=logger.name)
            logger.debug(
                "[_send_many_job_inputs] "
                f"{workdir_local=} compressed to {tar_path_local=}."
            )
            self.fractal_ssh.send_file(
                local=tar_path_local.as_posix(),
                remote=tar_path_remote.as_posix(),
            )
            logger.debug(
                "[_send_many_job_inputs] "
                f"{tar_path_local=} sent via SSH to {tar_path_remote=}."
            )
            self.fractal_ssh.run_command(cmd=tar_extraction_cmd)
            logger.debug(
                "[_send_many_job_inputs] "
                f"{tar_path_remote=} extracted to {workdir_remote=}."
            )
            self.fractal_ssh.run_command(cmd=rm_tar_cmd)
            logger.debug(
                "[_send_many_job_inputs] "
                f"{tar_path_remote=} removed from remote server."
            )
        except Exception as e:
            raise e
        finally:
            Path(tar_path_local).unlink(missing_ok=True)
            logger.debug(f"[_send_many_job_inputs] {tar_path_local=} removed.")

        logger.debug("[_send_many_job_inputs] END.")

    @override
    def run_squeue(self: Self, *, job_ids: list[str]) -> str:
        """
        Run `squeue` for a set of SLURM job IDs.

        Different scenarios:

        1. When `squeue -j` succeeds (with exit code 0), return its stdout.
        2. When `squeue -j` fails (typical example:
           `squeue -j {invalid_job_id}` fails with exit code 1), re-raise.
           The error will be handled upstream.
        3. When the SSH command fails because another thread is keeping the
           lock of the `FractalSSH` object for a long time, mock the standard
           output of the `squeue` command so that it looks like jobs are not
           completed yet.
        4. When the SSH command fails for other reasons (transport-level SSH
           failure or any other unexpected error), return the same placeholder
           as in (3) and attempt to refresh the SSH connection, so jobs are
           not wrongly marked as completed and the next poll can retry.
        """

        if len(job_ids) == 0:
            return ""

        job_id_single_str = ",".join([str(j) for j in job_ids])
        cmd = (
            "squeue --noheader --format='%i %T' --states=all "
            f"--jobs={job_id_single_str}"
        )

        try:
            stdout = self.fractal_ssh.run_command(
                cmd=cmd,
            )
            return stdout
        except FractalSSHCommandError as e:
            # Genuine `squeue` failure (e.g. invalid job id). Let upstream
            # decide (`_get_finished_jobs` falls back to per-job queries).
            raise e
        except FractalSSHTimeoutError:
            logger.warning(
                "[run_squeue] Could not acquire lock, use stdout placeholder."
            )
            return "\n".join(
                f"{job_id} {_SQUEUE_STATUS_PLACEHOLDER}" for job_id in job_ids
            )
        except _SSH_TRANSPORT_ERRORS as e:
            logger.error(
                "[run_squeue] Transport-level SSH failure "
                f"({type(e).__name__}): {e}. "
                "Returning placeholder so that jobs are NOT marked as "
                "completed. Will try to refresh SSH connection."
            )
            try:
                self.fractal_ssh.check_connection()
            except Exception as reconnect_err:
                logger.warning(
                    "[run_squeue] SSH reconnection attempt failed: "
                    f"{reconnect_err}. Next poll cycle will try again."
                )
            return "\n".join(
                f"{job_id} {_SQUEUE_STATUS_PLACEHOLDER}" for job_id in job_ids
            )
        except Exception as e:
            logger.error(
                "[run_squeue] Unexpected failure "
                f"({type(e).__name__}): {e}. "
                "Returning placeholder (jobs will be re-polled)."
            )
            return "\n".join(
                f"{job_id} {_SQUEUE_STATUS_PLACEHOLDER}" for job_id in job_ids
            )
