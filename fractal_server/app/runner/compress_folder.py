import shlex
import shutil
import subprocess  # nosec
import sys
from pathlib import Path

from fractal_server.logger import get_logger
from fractal_server.logger import set_logger


def run_subprocess(cmd: str, logger_name: str):
    logger = get_logger(logger_name)
    try:
        res = subprocess.run(  # nosec
            shlex.split(cmd), check=True, capture_output=True, encoding="utf-8"
        )
        return res
    except subprocess.CalledProcessError as e:
        logger.debug(
            f"Command '{e.cmd}' returned non-zero exit status {e.returncode}."
        )
        logger.debug(f"stdout: {e.stdout}")
        logger.debug(f"stderr: {e.stderr}")
        raise e
    except Exception as e:
        logger.debug(f"An error occurred while running command: {cmd}")
        logger.debug(str(e))
        raise e


def copy_subfolder(src: Path, dest: Path, logger_name: str):
    cmd_cp = f"cp -r {src.as_posix()} {dest.as_posix()}"
    res = run_subprocess(cmd=cmd_cp, logger_name=logger_name)
    return res


def create_tar_archive(
    tarfile_path: Path, subfolder_path_tmp_copy: Path, logger_name: str
):
    logger = get_logger(logger_name)
    cmd_tar = (
        f"tar czf {tarfile_path} "
        "--exclude *sbatch --exclude *_in_*.pickle "
        f"--directory={subfolder_path_tmp_copy.as_posix()} "
        "."
    )
    logger.debug(f"[compress_folder.py] cmd tar:\n{cmd_tar}")
    res = run_subprocess(cmd=cmd_tar, logger_name=logger_name)
    return res


def remove_temp_subfolder(subfolder_path_tmp_copy: Path, logger_name: str):
    logger = get_logger(logger_name)
    try:
        shutil.rmtree(subfolder_path_tmp_copy)
    except Exception as e:
        logger.debug(f"[compress_folder.py] ERROR during shutil.rmtree: {e}")


def compress_folder(
    subfolder_path: Path,
):
    logger = set_logger(__name__)

    logger.debug("[compress_folder.py] START")
    logger.debug(f"[compress_folder.py] {subfolder_path=}")
    job_folder = subfolder_path.parent
    subfolder_name = subfolder_path.name
    tarfile_path = (job_folder / f"{subfolder_name}.tar.gz").as_posix()
    logger.debug(f"[compress_folder.py] {tarfile_path=}")

    subfolder_path_tmp_copy = (
        subfolder_path.parent / f"{subfolder_path.name}_copy"
    )
    try:
        copy_subfolder(
            subfolder_path, subfolder_path_tmp_copy, logger_name=logger.name
        )
        res = create_tar_archive(
            tarfile_path, subfolder_path_tmp_copy, logger_name=logger.name
        )
        if res.returncode != 0:
            raise Exception(f"Error in tar command: {res.stderr}")

    except Exception as e:
        logger.debug(f"[compress_folder.py] ERROR: {e}")
        shutil.rmtree(subfolder_path_tmp_copy)
        sys.exit(1)

    finally:
        remove_temp_subfolder(subfolder_path_tmp_copy, logger_name=logger.name)


if __name__ == "__main__":

    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder"
    )

    if len(sys.argv[1:]) != 1:
        raise ValueError(
            "Invalid argument(s).\n" f"{help_msg}\n" f"Provided: {sys.argv=}"
        )
    subfolder_path = Path(sys.argv[1])

    compress_folder(
        subfolder_path=subfolder_path,
    )
