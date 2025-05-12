import logging
import os
import shlex
import subprocess  # nosec
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from ..slurm_common.base_slurm_runner import BaseSlurmRunner
from ..slurm_common.slurm_job_task_models import SlurmJob
from ._subprocess_run_as_user import _mkdir_as_user
from ._subprocess_run_as_user import _run_command_as_user
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)


def _subprocess_run_or_raise(
    full_command: str,
) -> subprocess.CompletedProcess | None:
    try:
        output = subprocess.run(  # nosec
            shlex.split(full_command),
            capture_output=True,
            check=True,
            encoding="utf-8",
        )
        return output
    except subprocess.CalledProcessError as e:
        error_msg = (
            f"Submit command `{full_command}` failed. "
            f"Original error:\n{str(e)}\n"
            f"Original stdout:\n{e.stdout}\n"
            f"Original stderr:\n{e.stderr}\n"
        )
        logging.error(error_msg)
        raise JobExecutionError(info=error_msg)


class SudoSlurmRunner(BaseSlurmRunner):
    slurm_user: str
    slurm_account: str | None = None

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
        slurm_account: str | None = None,
        slurm_user: str,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """

        self.slurm_user = slurm_user
        self.slurm_account = slurm_account
        settings = Inject(get_settings)

        super().__init__(
            slurm_runner_type="sudo",
            root_dir_local=root_dir_local,
            root_dir_remote=root_dir_remote,
            common_script_lines=common_script_lines,
            user_cache_dir=user_cache_dir,
            poll_interval=poll_interval,
            python_worker_interpreter=(
                settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
            ),
        )

    def _mkdir_local_folder(self, folder: str) -> None:
        original_umask = os.umask(0)
        Path(folder).mkdir(parents=True, mode=0o755)
        os.umask(original_umask)

    def _mkdir_remote_folder(self, folder: str) -> None:
        _mkdir_as_user(folder=folder, user=self.slurm_user)

    def _fetch_artifacts_single_job(self, job: SlurmJob) -> None:
        """
        Fetch artifacts for a single SLURM jobs.
        """
        logger.debug(
            f"[_fetch_artifacts_single_job] {job.slurm_job_id=} START"
        )
        source_target_list = [
            (job.slurm_stdout_remote, job.slurm_stdout_local),
            (job.slurm_stderr_remote, job.slurm_stderr_local),
        ]
        for task in job.tasks:
            source_target_list.extend(
                [
                    (
                        task.output_file_remote,
                        task.output_file_local,
                    ),
                    (
                        task.task_files.log_file_remote,
                        task.task_files.log_file_local,
                    ),
                    (
                        task.task_files.metadiff_file_remote,
                        task.task_files.metadiff_file_local,
                    ),
                ]
            )

        for source, target in source_target_list:
            try:
                res = _run_command_as_user(
                    cmd=f"cat {source}",
                    user=self.slurm_user,
                    check=True,
                )
                # Write local file
                with open(target, "w") as f:
                    f.write(res.stdout)
                logger.debug(
                    f"[_fetch_artifacts_single_job] Copied {source} into "
                    f"{target}"
                )
            except RuntimeError as e:
                logger.warning(
                    f"SKIP copy {source} into {target}. "
                    f"Original error: {str(e)}"
                )
        logger.debug(f"[_fetch_artifacts_single_job] {job.slurm_job_id=} END")

    def _fetch_artifacts(
        self,
        finished_slurm_jobs: list[SlurmJob],
    ) -> None:
        """
        Fetch artifacts for a list of SLURM jobs.
        """
        MAX_NUM_THREADS = 12
        THREAD_NAME_PREFIX = "fetch_artifacts"
        logger.debug(
            "[_fetch_artifacts] START "
            f"({MAX_NUM_THREADS=}, {len(finished_slurm_jobs)=})."
        )
        with ThreadPoolExecutor(
            max_workers=MAX_NUM_THREADS,
            thread_name_prefix=THREAD_NAME_PREFIX,
        ) as executor:
            executor.map(
                self._fetch_artifacts_single_job,
                finished_slurm_jobs,
            )
        logger.debug("[_fetch_artifacts] END.")

    def _run_remote_cmd(self, cmd: str) -> str:
        res = _run_command_as_user(
            cmd=cmd,
            user=self.slurm_user,
            check=True,
        )
        return res.stdout

    def run_squeue(self, job_ids: list[str]) -> str:
        """
        Run `squeue` for a set of SLURM job IDs.
        """

        if len(job_ids) == 0:
            return ""

        job_id_single_str = ",".join([str(j) for j in job_ids])
        cmd = (
            "squeue --noheader --format='%i %T' --states=all "
            f"--jobs {job_id_single_str}"
        )
        res = _subprocess_run_or_raise(cmd)
        return res.stdout
