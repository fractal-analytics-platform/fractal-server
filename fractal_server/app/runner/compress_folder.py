"""
Wrap `tar` compression command.

This module is used both locally (in the environment where `fractal-server`
is running) and remotely (as a standalon Python module, executed over SSH).

This is a twin-module of `extract_archive.py`.

The reason for using the `tar` command via `subprocess` rather than Python
built-in `tarfile` library has to do with performance issues we observed
when handling files which were just created within a SLURM job, and in the
context of a CephFS filesystem.
"""
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
        f"tar czf {tarfile_path} --overwrite-dir "
        "--exclude *sbatch --exclude *_in_*.pickle "
        f"--directory={subfolder_path_tmp_copy.as_posix()} "
        "."
    )
    logger.debug(f"cmd tar:\n{cmd_tar}")
    run_subprocess(cmd=cmd_tar, logger_name=logger_name)


def remove_temp_subfolder(subfolder_path_tmp_copy: Path, logger_name: str):
    logger = get_logger(logger_name)
    try:
        shutil.rmtree(subfolder_path_tmp_copy)
    except Exception as e:
        logger.debug(f"ERROR during shutil.rmtree: {e}")


def compress_folder(subfolder_path: Path):
    """
    FIXME
    """

    logger_name = "compress_folder"
    logger = set_logger(logger_name)

    logger.debug("START")
    logger.debug(f"{subfolder_path=}")
    parent_dir = subfolder_path.parent
    subfolder_name = subfolder_path.name
    tarfile_path = (parent_dir / f"{subfolder_name}.tar.gz").as_posix()
    logger.debug(f"{tarfile_path=}")

    subfolder_path_tmp_copy = (
        subfolder_path.parent / f"{subfolder_path.name}_copy"
    )
    try:
        copy_subfolder(
            subfolder_path, subfolder_path_tmp_copy, logger_name=logger_name
        )
        create_tar_archive(
            tarfile_path, subfolder_path_tmp_copy, logger_name=logger_name
        )

    except Exception as e:
        logger.debug(f"ERROR: {e}")
        sys.exit(1)

    finally:
        remove_temp_subfolder(subfolder_path_tmp_copy, logger_name=logger_name)


def main(sys_argv: list[str]):

    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder"
    )
    if len(sys_argv[1:]) != 1:
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")

    subfolder_path = Path(sys_argv[1])
    compress_folder(
        subfolder_path=subfolder_path,
    )


if __name__ == "__main__":
    main(sys.argv)
