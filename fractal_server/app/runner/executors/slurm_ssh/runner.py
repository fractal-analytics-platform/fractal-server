import time
from pathlib import Path

from ..slurm_common.base_slurm_runner import BaseSlurmRunner
from ..slurm_common.slurm_job_task_models import SlurmJob
from .run_subprocess import run_subprocess
from .tar_commands import get_tar_compression_cmd
from .tar_commands import get_tar_extraction_cmd
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHCommandError
from fractal_server.ssh._fabric import FractalSSHTimeoutError
from fractal_server.syringe import Inject


logger = set_logger(__name__)


class SlurmSSHRunner(BaseSlurmRunner):
    fractal_ssh: FractalSSH

    def __init__(
        self,
        *,
        # Common
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: list[str] | None = None,
        user_cache_dir: str | None = None,
        poll_interval: int | None = None,
        # Specific
        fractal_ssh: FractalSSH,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """
        self.fractal_ssh = fractal_ssh
        logger.warning(self.fractal_ssh)

        settings = Inject(get_settings)

        super().__init__(
            slurm_runner_type="ssh",
            root_dir_local=root_dir_local,
            root_dir_remote=root_dir_remote,
            common_script_lines=common_script_lines,
            user_cache_dir=user_cache_dir,
            poll_interval=poll_interval,
            python_worker_interpreter=settings.FRACTAL_SLURM_WORKER_PYTHON,
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
        logger.debug(
            f"[_fetch_artifacts] START ({len(finished_slurm_jobs)=})."
        )

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

    def _run_remote_cmd(self, cmd: str) -> str:
        stdout = self.fractal_ssh.run_command(cmd=cmd)
        return stdout

    def run_squeue(
        self,
        *,
        job_ids: list[str],
        base_interval: float = 2.0,
        max_attempts: int = 7,
    ) -> str:
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
                base_interval=base_interval,
                max_attempts=max_attempts,
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
