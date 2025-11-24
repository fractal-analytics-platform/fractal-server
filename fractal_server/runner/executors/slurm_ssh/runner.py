import time
from pathlib import Path

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

from .run_subprocess import run_subprocess
from .tar_commands import get_tar_compression_cmd
from .tar_commands import get_tar_extraction_cmd
from .tar_commands import get_tar_extraction_cmd_with_target_folder

logger = set_logger(__name__)


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
        )

    def _mkdir_local_folder(self, folder: str) -> None:
        Path(folder).mkdir(parents=True)

    def _mkdir_remote_folder(self, folder: str):
        self.fractal_ssh.mkdir(
            folder=folder,
            parents=True,
        )

    def _fetch_artifacts(
        self,
        finished_slurm_jobs: list[SlurmJob],
    ) -> None:
        """
        Fetch artifacts for a list of SLURM jobs.
        """

        # Check length
        if len(finished_slurm_jobs) == 0:
            logger.debug(f"[_fetch_artifacts] EXIT ({finished_slurm_jobs=}).")
            return None

        t_0 = time.perf_counter()
        logger.debug(f"[_fetch_artifacts] START ({len(finished_slurm_jobs)=}).")

        for slurm_job in finished_slurm_jobs:
            # Fetch archive
            self.fractal_ssh.fetch_file(
                remote=slurm_job.tar_path_remote.as_posix(),
                local=slurm_job.tar_path_local.as_posix(),
            )
            # Remove remote archive
            rm_tar_cmd = f"rm {slurm_job.tar_path_remote.as_posix()}"
            self.fractal_ssh.run_command(cmd=rm_tar_cmd)
            # Extract archive locally
            cmd_tar = get_tar_extraction_cmd_with_target_folder(
                archive_path=slurm_job.tar_path_local,
                target_folder=slurm_job.workdir_local,
            )
            run_subprocess(cmd=cmd_tar, logger_name=logger.name)
            # Remove local archive
            Path(slurm_job.tar_path_local).unlink(missing_ok=True)

        t_1 = time.perf_counter()
        logger.info(f"[_fetch_artifacts] End - elapsed={t_1 - t_0:.3f} s")

    def _run_remote_cmd(self, cmd: str) -> str:
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

    def run_squeue(self, *, job_ids: list[str]) -> str:
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
        4. When the SSH command fails for other reasons, despite a forgiving
           setup (7 connection attempts with base waiting interval of 2
           seconds, with a cumulative timeout of 126 seconds), return an empty
           string. This will be treated upstream as an empty `squeu` output,
           indirectly resulting in marking the job as completed.
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
            raise e
        except FractalSSHTimeoutError:
            logger.warning(
                "[run_squeue] Could not acquire lock, use stdout placeholder."
            )
            FAKE_STATUS = "FRACTAL_STATUS_PLACEHOLDER"
            placeholder_stdout = "\n".join(
                [f"{job_id} {FAKE_STATUS}" for job_id in job_ids]
            )
            return placeholder_stdout
        except Exception as e:
            logger.error(f"Ignoring `squeue` command failure {e}")
            return ""
