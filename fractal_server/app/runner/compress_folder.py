import shutil
import sys
from pathlib import Path

from fractal_server.app.runner.run_subprocess import run_subprocess
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger


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
        sys.exit(1)

    finally:
        remove_temp_subfolder(subfolder_path_tmp_copy, logger_name=logger.name)


def main(sys_argv: list[str]):

    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder"
    )
    if len(sys_argv) != 1:
        print("Invalid argument(s).")
        print(f"{help_msg}")
        print(f"Provided: {sys_argv=}")
        sys.exit(1)

    subfolder_path = Path(sys_argv[0])
    compress_folder(
        subfolder_path=subfolder_path,
    )


if __name__ == "__main__":
    main(sys.argv)
