import time
from pathlib import Path
from typing import Optional

from ..slurm_common.base_slurm_runner import BaseSlurmRunner
from ..slurm_common.slurm_job_task_models import SlurmJob
from fractal_server.app.runner.compress_folder import compress_folder
from fractal_server.app.runner.extract_archive import extract_archive
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
        # Common
        root_dir_local: Path,
        root_dir_remote: Path,
        common_script_lines: Optional[list[str]] = None,
        user_cache_dir: Optional[str] = None,
        poll_interval: Optional[int] = None,
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
        tarfile_path_local = (
            workdir_local.parent / f"{workdir_local.name}.tar.gz"
        ).as_posix()
        tarfile_path_remote = (
            workdir_remote.parent / f"{workdir_remote.name}.tar.gz"
        ).as_posix()

        # Create file list
        # # FIXME can we make this more efficient with iterators?
        filelist = []
        for _slurm_job in finished_slurm_jobs:
            _single_job_filelist = [
                _slurm_job.slurm_stdout_remote_path.name,
                _slurm_job.slurm_stderr_remote_path.name,
            ]
            for task in _slurm_job.tasks:
                _single_job_filelist.extend(
                    [
                        task.output_pickle_file_remote_path.name,
                        task.task_files.log_file_remote_path.name,
                        task.task_files.args_file_remote_path.name,
                        task.task_files.metadiff_file_remote_path.name,
                    ]
                )
            filelist.extend(_single_job_filelist)
        filelist_string = "\n".join(filelist)
        elapsed = time.perf_counter() - t_0
        logger.debug(
            "[_fetch_artifacts] Created filelist "
            f"({len(filelist)=}, from start: {elapsed:.3f} s)."
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
            f"(from start: {elapsed:.3f} s)."
        )

        # Create remote tarfile
        t_0_tar = time.perf_counter()
        tar_command = (
            f"{self.python_worker_interpreter} "
            "-m fractal_server.app.runner.compress_folder "
            f"{workdir_remote.as_posix()} "
            f"--filelist {tmp_filelist_path}"
        )
        self.fractal_ssh.run_command(cmd=tar_command)
        t_1_tar = time.perf_counter()
        logger.info(
            f"Remote archive {tarfile_path_remote} created"
            f" - elapsed: {t_1_tar - t_0_tar:.3f} s"
        )

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
        Path(tarfile_path_local).unlink(missing_ok=True)

        t_1 = time.perf_counter()
        logger.info(f"[_get_subfolder_sftp] End - elapsed: {t_1 - t_0:.3f} s")

    def _send_inputs(self, jobs: list[SlurmJob]) -> None:
        """
        Transfer the jobs subfolder to the remote host.
        """
        for job in jobs:

            # Create local archive
            tarfile_path_local = compress_folder(
                job.workdir_local,
                filelist_path=None,
            )
            tarfile_name = Path(tarfile_path_local).name
            logger.info(f"Subfolder archive created at {tarfile_path_local}")

            # Transfer archive
            tarfile_path_remote = (
                job.workdir_remote.parent / tarfile_name
            ).as_posix()
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

            # Remove local archive
            Path(tarfile_path_local).unlink()
            logger.debug(f"Local archive {tarfile_path_local} removed")

            # Uncompress remote archive
            tar_command = (
                f"{self.python_worker_interpreter} -m "
                "fractal_server.app.runner.extract_archive "
                f"{tarfile_path_remote}"
            )
            self.fractal_ssh.run_command(cmd=tar_command)

    def _run_remote_cmd(self, cmd: str) -> str:
        stdout = self.fractal_ssh.run_command(cmd=cmd)
        return stdout
