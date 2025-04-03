import logging
import os
import shlex
import subprocess  # nosec
import sys
from copy import copy
from pathlib import Path
from typing import Optional

import cloudpickle

from ..slurm_common._slurm_config import SlurmConfig
from ..slurm_common.base_slurm_runner import BaseSlurmRunner
from ..slurm_common.slurm_job_task_models import SlurmJob
from ._subprocess_run_as_user import _mkdir_as_user
from ._subprocess_run_as_user import _run_command_as_user
from fractal_server import __VERSION__
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject


logger = set_logger(__name__)


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

    def _submit_single_sbatch(
        self,
        func,
        slurm_job: SlurmJob,
        slurm_config: SlurmConfig,
    ) -> str:
        logger.debug("[_submit_single_sbatch] START")
        # Prepare input pickle(s)
        versions = dict(
            python=sys.version_info[:3],
            cloudpickle=cloudpickle.__version__,
            fractal_server=__VERSION__,
        )
        for task in slurm_job.tasks:
            _args = []
            _kwargs = dict(
                parameters=task.parameters,
                remote_files=task.task_files.remote_files_dict,
            )
            funcser = cloudpickle.dumps((versions, func, _args, _kwargs))
            with open(task.input_pickle_file_local, "wb") as f:
                f.write(funcser)
            logger.debug(
                "[_submit_single_sbatch] Written "
                f"{task.input_pickle_file_local=}"
            )
        # Prepare commands to be included in SLURM submission script
        cmdlines = []
        for task in slurm_job.tasks:
            input_pickle_file = task.input_pickle_file_local
            output_pickle_file = task.output_pickle_file_remote
            cmdlines.append(
                (
                    f"{self.python_worker_interpreter}"
                    " -m fractal_server.app.runner."
                    "executors.slurm_common.remote "
                    f"--input-file {input_pickle_file} "
                    f"--output-file {output_pickle_file}"
                )
            )

        # ...
        num_tasks_max_running = slurm_config.parallel_tasks_per_job
        mem_per_task_MB = slurm_config.mem_per_task_MB

        # Set ntasks
        ntasks = min(len(cmdlines), num_tasks_max_running)
        slurm_config.parallel_tasks_per_job = ntasks

        # Prepare SLURM preamble based on SlurmConfig object
        script_lines = slurm_config.to_sbatch_preamble(
            remote_export_dir=self.user_cache_dir
        )

        # Extend SLURM preamble with variable which are not in SlurmConfig, and
        # fix their order
        script_lines.extend(
            [
                f"#SBATCH --out={slurm_job.slurm_stdout_remote}",
                f"#SBATCH --err={slurm_job.slurm_stderr_remote}",
                f"#SBATCH -D {slurm_job.workdir_remote}",
            ]
        )
        script_lines = slurm_config.sort_script_lines(script_lines)
        logger.debug(script_lines)

        # Always print output of `uname -n` and `pwd`
        script_lines.append(
            '"Hostname: `uname -n`; current directory: `pwd`"\n'
        )

        # Complete script preamble
        script_lines.append("\n")

        # Include command lines
        tmp_list_commands = copy(cmdlines)
        while tmp_list_commands:
            if tmp_list_commands:
                cmd = tmp_list_commands.pop(0)  # take first element
                script_lines.append(
                    "srun --ntasks=1 --cpus-per-task=$SLURM_CPUS_PER_TASK "
                    f"--mem={mem_per_task_MB}MB "
                    f"{cmd} &"
                )
        script_lines.append("wait\n")

        script = "\n".join(script_lines)

        # Write submission script
        # submission_script_contents = "\n".join(preamble_lines + cmdlines)
        with open(slurm_job.slurm_submission_script_local, "w") as f:
            f.write(script)

        # Run sbatch
        pre_command = f"sudo --set-home --non-interactive -u {self.slurm_user}"
        submit_command = (
            f"sbatch --parsable {slurm_job.slurm_submission_script_local}"
        )
        full_command = f"{pre_command} {submit_command}"

        # Submit SLURM job and retrieve job ID
        res = _subprocess_run_or_raise(full_command)
        submitted_job_id = int(res.stdout)
        slurm_job.slurm_job_id = str(submitted_job_id)

        # Add job to self.jobs
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.debug(f"Added {slurm_job.slurm_job_id} to self.jobs.")

    def _copy_files_from_remote_to_local(self, job: SlurmJob) -> None:
        """
        Note: this would differ for SSH
        """
        logger.debug(f"[_copy_files_from_remote_to_local] {job.slurm_job_id=}")
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

    def _run_single_cmd(self, cmd: str):
        res = _subprocess_run_or_raise(
            (
                f"{self.python_worker_interpreter} "
                "-m fractal_server.app.runner.versions"
            )
        )
        return res.stdout
