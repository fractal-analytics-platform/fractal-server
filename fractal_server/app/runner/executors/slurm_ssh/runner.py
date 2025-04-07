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
        self.python_worker_interpreter = settings.FRACTAL_SLURM_WORKER_PYTHON

        super().__init__(
            slurm_runner_type="ssh",
            root_dir_local=root_dir_local,
            root_dir_remote=root_dir_remote,
            common_script_lines=common_script_lines,
            user_cache_dir=user_cache_dir,
            poll_interval=poll_interval,
        )

    def _mkdir_local_folder(self, folder: str) -> None:
        Path(folder).mkdir(parents=True)

    def _mkdir_remote_folder(self, folder: str):
        self.fractal_ssh.mkdir(
            folder=folder,
            parents=True,
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

        # Create local archive
        tarfile_path_local = compress_folder(job.workdir_local)
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
            logger.info(f"Removed {tarfile_path_remote=}")
        except RuntimeError as e:
            logger.info(
                f"Could not remove {tarfile_path_remote=}.\n"
                f"Original error: {str(e)}"
            )

        # Create remote tarfile
        # FIXME: introduce filtering by prefix, so that when the subfolder
        # includes N SLURM jobs we don't always copy the cumulative folder
        # but only the relevant part
        t_0_tar = time.perf_counter()
        tar_command = (
            f"{self.python_worker_interpreter} "
            "-m fractal_server.app.runner.compress_folder "
            f"{job.workdir_remote.as_posix()} "
            "--remote-to-local"
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

    def _run_remote_cmd(self, cmd: str) -> str:
        stdout = self.fractal_ssh.run_command(cmd=cmd)
        return stdout
