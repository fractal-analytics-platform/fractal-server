import logging
import os
import shlex
import subprocess  # nosec
import sys
from pathlib import Path
from typing import Optional

from ..slurm_common.base_slurm_runner import BaseSlurmRunner
from ..slurm_common.slurm_job_task_models import SlurmJob
from ._subprocess_run_as_user import _mkdir_as_user
from ._subprocess_run_as_user import _run_command_as_user
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject


logger = set_logger(__name__)
# FIXME: Transform several logger.info into logger.debug.


def _subprocess_run_or_raise(
    full_command: str,
) -> Optional[subprocess.CompletedProcess]:
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
    slurm_account: Optional[str] = None

    def __init__(
        self,
        *,
        # Common
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: Optional[list[str]] = None,
        user_cache_dir: Optional[str] = None,
        poll_interval: Optional[int] = None,
        # Specific
        slurm_account: Optional[str] = None,
        slurm_user: str,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """

        self.slurm_user = slurm_user
        self.slurm_account = slurm_account
        settings = Inject(get_settings)

        self.python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        super().__init__(
            slurm_runner_type="sudo",
            root_dir_local=root_dir_local,
            root_dir_remote=root_dir_remote,
            common_script_lines=common_script_lines,
            user_cache_dir=user_cache_dir,
            poll_interval=poll_interval,
        )

    def _mkdir_local_folder(self, folder: str) -> None:
        original_umask = os.umask(0)
        Path(folder).mkdir(parents=True, mode=0o755)
        os.umask(original_umask)

    def _mkdir_remote_folder(self, folder: str) -> None:
        _mkdir_as_user(folder=folder, user=self.slurm_user)

    def _copy_files_from_remote_to_local(self, job: SlurmJob) -> None:
        """
        Note: this would differ for SSH
        """
        logger.info(f"[_copy_files_from_remote_to_local] {job.slurm_job_id=}")
        source_target_list = [
            (job.slurm_stdout_remote, job.slurm_stdout_local),
            (job.slurm_stderr_remote, job.slurm_stderr_local),
        ]
        for task in job.tasks:
            source_target_list.extend(
                [
                    (
                        task.output_pickle_file_remote,
                        task.output_pickle_file_local,
                    ),
                    (
                        task.task_files.log_file_remote,
                        task.task_files.log_file_local,
                    ),
                    (
                        task.task_files.args_file_remote,
                        task.task_files.args_file_local,
                    ),
                    (
                        task.task_files.metadiff_file_remote,
                        task.task_files.metadiff_file_local,
                    ),
                ]
            )

        for source, target in source_target_list:
            # NOTE: By setting encoding=None, we read/write bytes instead
            # of strings; this is needed to also handle pickle files.
            try:
                res = _run_command_as_user(
                    cmd=f"cat {source}",
                    user=self.slurm_user,
                    encoding=None,
                    check=True,
                )
                # Write local file
                with open(target, "wb") as f:
                    f.write(res.stdout)
                logger.critical(f"Copied {source} into {target}")
            except RuntimeError as e:
                logger.warning(
                    f"SKIP copy {source} into {target}. "
                    f"Original error: {str(e)}"
                )

    def _run_remote_cmd(self, cmd: str):
        res = _run_command_as_user(
            cmd=cmd,
            user=self.slurm_user,
            encoding="utf-8",
            check=True,
        )
        return res.stdout

    def _run_local_cmd(self, cmd: str):
        res = _subprocess_run_or_raise(cmd)
        return res.stdout
