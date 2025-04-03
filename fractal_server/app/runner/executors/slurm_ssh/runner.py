import json
import sys
import time
from copy import copy
from pathlib import Path
from typing import Optional

import cloudpickle

from ..slurm_common._slurm_config import SlurmConfig
from ..slurm_common.base_slurm_runner import BaseSlurmRunner
from ..slurm_common.slurm_job_task_models import SlurmJob
from ._check_job_status_ssh import get_finished_jobs_ssh
from fractal_server import __VERSION__
from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.extract_archive import extract_archive
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject


logger = set_logger(__name__)


class SlurmSSHRunner(BaseSlurmRunner):
    fractal_ssh: FractalSSH

    def __init__(
        self,
        *,
        fractal_ssh: FractalSSH,
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: Optional[list[str]] = None,
        user_cache_dir: Optional[str] = None,
        poll_interval: Optional[int] = None,
    ) -> None:
        """
        Set parameters that are the same for different Fractal tasks and for
        different SLURM jobs/tasks.
        """

        self.common_script_lines = common_script_lines or []

        # Check that SLURM account is not set here
        # FIXME: move to little method
        try:
            invalid_line = next(
                line
                for line in self.common_script_lines
                if line.startswith("#SBATCH --account=")
            )
            raise RuntimeError(
                "Invalid line in `RunnerSlurmSSH.common_script_lines`: "
                f"'{invalid_line}'.\n"
                "SLURM account must be set via the request body of the "
                "apply-workflow endpoint, or by modifying the user properties."
            )
        except StopIteration:
            pass

        # Check Python versions
        self.fractal_ssh = fractal_ssh
        logger.warning(self.fractal_ssh)

        settings = Inject(get_settings)
        # It is the new handshanke
        if settings.FRACTAL_SLURM_WORKER_PYTHON is not None:
            self.check_remote_python_interpreter()

        # Initialize connection and perform handshake
        self.root_dir_local = root_dir_local
        self.root_dir_remote = root_dir_remote

        # # Create folders
        # original_umask = os.umask(0)
        # self.root_dir_local.mkdir(parents=True, exist_ok=True, mode=0o755)
        # os.umask(original_umask)
        # _mkdir_as_user(
        #     folder=self.root_dir_remote.as_posix(),
        #     user=self.slurm_user,
        # )

        self.user_cache_dir = user_cache_dir

        self.poll_interval = (
            poll_interval or settings.FRACTAL_SLURM_POLL_INTERVAL
        )

        self.shutdown_file = self.root_dir_local / SHUTDOWN_FILENAME

        self.python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )

        self.jobs = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def _mkdir_local_folder(self, folder: str) -> None:
        Path(folder).mkdir(parents=True)

    def _mkdir_remote_folder(self, folder: str):
        self.fractal_ssh.mkdir(
            folder=folder,
            parents=True,
        )

    def scancel_jobs(self) -> None:
        logger.debug("[scancel_jobs] START")

        if self.jobs:
            scancel_string = " ".join(self.job_ids)
            scancel_cmd = f"scancel {scancel_string}"
            logger.warning(f"Now scancel-ing SLURM jobs {scancel_string}")
            try:
                self.fractal_ssh.run_command(cmd=scancel_cmd)
            except RuntimeError as e:
                logger.warning(
                    "[scancel_jobs] `scancel` command failed. "
                    f"Original error:\n{str(e)}"
                )

        logger.debug("[scancel_jobs] END")

    def _get_finished_jobs(
        self,
        job_ids: list[str],
    ) -> set[str]:
        return get_finished_jobs_ssh(
            job_ids=job_ids,
            fractal_ssh=self.fractal_ssh,
        )

    def _copy_files_from_remote_to_local(self, slurm_job: SlurmJob) -> None:
        self._get_subfolder_sftp(job=slurm_job)

    def _put_subfolder_sftp(self, job: SlurmJob) -> None:
        # FIXME re-introduce use of this function, but only after splitting
        # submission logic into
        # 1. prepare all
        # 2. send folder
        # 3. submit all
        """
        Transfer the jobs subfolder to the remote host.
        """

        # Create compressed subfolder archive (locally)
        tarfile_path_local = compress_folder(job.workdir_local)

        tarfile_name = Path(tarfile_path_local).name
        logger.info(f"Subfolder archive created at {tarfile_path_local}")
        tarfile_path_remote = (
            job.workdir_remote.parent / tarfile_name
        ).as_posix()
        # Transfer archive
        t_0_put = time.perf_counter()
        self.fractal_ssh.send_file(
            local=tarfile_path_local,
            remote=tarfile_path_remote,
        )
        t_1_put = time.perf_counter()
        logger.info(
            f"Subfolder archive transferred to {tarfile_path_remote}"
            f" - elapsed: {t_1_put - t_0_put:.3f} s"
        )
        # Uncompress archive (remotely)
        tar_command = (
            f"{self.python_worker_interpreter} -m "
            "fractal_server.app.runner.extract_archive "
            f"{tarfile_path_remote}"
        )
        self.fractal_ssh.run_command(cmd=tar_command)

        # Remove local version
        t_0_rm = time.perf_counter()
        Path(tarfile_path_local).unlink()
        t_1_rm = time.perf_counter()
        logger.info(
            f"Local archive removed - elapsed: {t_1_rm - t_0_rm:.3f} s"
        )

    def _get_subfolder_sftp(self, job: SlurmJob) -> None:
        """
        Fetch a remote folder via tar+sftp+tar
        """

        t_0 = time.perf_counter()
        logger.debug("[_get_subfolder_sftp] Start")
        tarfile_path_local = (
            job.workdir_local.parent / f"{job.workdir_local.name}.tar.gz"
        ).as_posix()
        tarfile_path_remote = (
            job.workdir_remote.parent / f"{job.workdir_remote.name}.tar.gz"
        ).as_posix()

        # Remove remote tarfile
        try:
            rm_command = f"rm {tarfile_path_remote}"
            self.fractal_ssh.run_command(cmd=rm_command)
        except RuntimeError as e:
            logger.warning(f"{tarfile_path_remote} already exists!\n {str(e)}")

        # Create remote tarfile
        # FIXME: introduce filtering by prefix, so that when the subfolder
        # includes N SLURM jobs we don't always copy the cumulative folder
        # but only the relevant part
        tar_command = (
            f"{self.python_worker_interpreter} "
            "-m fractal_server.app.runner.compress_folder "
            f"{job.workdir_remote.as_posix()} "
            "--remote-to-local"
        )
        stdout = self.fractal_ssh.run_command(cmd=tar_command)
        print(stdout)

        # Fetch tarfile
        t_0_get = time.perf_counter()
        self.fractal_ssh.fetch_file(
            remote=tarfile_path_remote,
            local=tarfile_path_local,
        )
        t_1_get = time.perf_counter()
        logger.info(
            f"Subfolder archive transferred back to {tarfile_path_local}"
            f" - elapsed: {t_1_get - t_0_get:.3f} s"
        )

        # Extract tarfile locally
        extract_archive(Path(tarfile_path_local))

        # Remove local tarfile
        if Path(tarfile_path_local).exists():
            logger.warning(f"Remove existing file {tarfile_path_local}.")
            Path(tarfile_path_local).unlink()

        t_1 = time.perf_counter()
        logger.info(f"[_get_subfolder_sftp] End - elapsed: {t_1 - t_0:.3f} s")

    def _submit_single_sbatch(
        self,
        func,
        slurm_job: SlurmJob,
        slurm_config: SlurmConfig,
    ) -> str:
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
            # Send input pickle
            self.fractal_ssh.send_file(
                local=task.input_pickle_file_local,
                remote=task.input_pickle_file_remote,
            )
        # Prepare commands to be included in SLURM submission script
        settings = Inject(get_settings)
        python_worker_interpreter = (
            settings.FRACTAL_SLURM_WORKER_PYTHON or sys.executable
        )
        cmdlines = []
        for task in slurm_job.tasks:
            input_pickle_file = task.input_pickle_file_remote
            output_pickle_file = task.output_pickle_file_remote
            cmdlines.append(
                (
                    f"{python_worker_interpreter}"
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
                f"#SBATCH --err={slurm_job.slurm_stderr_remote}",
                f"#SBATCH --out={slurm_job.slurm_stdout_remote}",
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

        self.fractal_ssh.send_file(
            local=slurm_job.slurm_submission_script_local,
            remote=slurm_job.slurm_submission_script_remote,
        )

        # Run sbatch
        submit_command = (
            f"sbatch --parsable {slurm_job.slurm_submission_script_remote}"
        )
        pre_submission_cmds = slurm_config.pre_submission_commands
        if len(pre_submission_cmds) == 0:
            sbatch_stdout = self.fractal_ssh.run_command(cmd=submit_command)
        else:
            logger.debug(f"Now using {pre_submission_cmds=}")
            script_lines = pre_submission_cmds + [submit_command]
            script_content = "\n".join(script_lines)
            script_content = f"{script_content}\n"
            script_path_remote = (
                f"{slurm_job.slurm_script_remote.as_posix()}_wrapper.sh"
            )
            self.fractal_ssh.write_remote_file(
                path=script_path_remote, content=script_content
            )
            cmd = f"bash {script_path_remote}"
            sbatch_stdout = self.fractal_ssh.run_command(cmd=cmd)

        # Submit SLURM job and retrieve job ID
        stdout = sbatch_stdout.strip("\n")
        submitted_job_id = int(stdout)
        slurm_job.slurm_job_id = str(submitted_job_id)

        # Add job to self.jobs
        self.jobs[slurm_job.slurm_job_id] = slurm_job
        logger.debug(f"Added {slurm_job.slurm_job_id} to self.jobs.")

    def check_remote_python_interpreter(self):
        settings = Inject(get_settings)
        cmd = (
            f"{settings.FRACTAL_SLURM_WORKER_PYTHON} "
            "-m fractal_server.app.runner.versions"
        )
        stdout = self.fractal_ssh.run_command(cmd=cmd)
        remote_version = json.loads(stdout.strip("\n"))["fractal_server"]
        if remote_version != __VERSION__:
            error_msg = (
                "Fractal-server version mismatch.\n"
                "Local interpreter: "
                f"({sys.executable}): {__VERSION__}.\n"
                "Remote interpreter: "
                f"({settings.FRACTAL_SLURM_WORKER_PYTHON}): {remote_version}."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)
