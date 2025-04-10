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
import sys
import time
from pathlib import Path

from fractal_server.app.runner.run_subprocess import run_subprocess
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger


def _copy_subfolder(src: Path, dest: Path, logger_name: str):
    t_start = time.perf_counter()
    cmd_cp = f"cp -r {src.as_posix()} {dest.as_posix()}"
    logger = get_logger(logger_name=logger_name)
    logger.debug(f"{cmd_cp=}")
    res = run_subprocess(cmd=cmd_cp, logger_name=logger_name)
    elapsed = time.perf_counter() - t_start
    logger.debug(f"[_copy_subfolder] {elapsed=} s ({dest=})")
    return res


def _create_tar_archive(
    tarfile_path: Path,
    subfolder_path_tmp_copy: Path,
    logger_name: str,
    remote_to_local: bool,
):
    logger = get_logger(logger_name)
    logger.debug(f"[_create_tar_archive] START ({tarfile_path.as_posix()})")
    t_start = time.perf_counter()

    if remote_to_local:
        exclude_options = "--exclude *sbatch --exclude *_in_*.pickle "
    else:
        exclude_options = ""

    cmd_tar = (
        f"tar czf {tarfile_path} "
        f"{exclude_options} "
        f"--directory={subfolder_path_tmp_copy.as_posix()} "
        "."
    )
    logger.debug(f"cmd tar:\n{cmd_tar}")
    run_subprocess(cmd=cmd_tar, logger_name=logger_name, allow_char="*")
    elapsed = time.perf_counter() - t_start
    logger.debug(
        f"[_create_tar_archive] END {elapsed=} s ({tarfile_path.as_posix()})"
    )


def _remove_temp_subfolder(subfolder_path_tmp_copy: Path, logger_name: str):
    logger = get_logger(logger_name)
    t_start = time.perf_counter()
    try:
        cmd_rm = f"rm -r {subfolder_path_tmp_copy}"
        logger.debug(f"cmd rm:\n{cmd_rm}")
        run_subprocess(cmd=cmd_rm, logger_name=logger_name, allow_char="*")
    except Exception as e:
        logger.debug(f"ERROR during {cmd_rm}: {e}")
    elapsed = time.perf_counter() - t_start
    logger.debug(
        f"[_copy_subfolder] {elapsed=} s ({subfolder_path_tmp_copy=})"
    )


def compress_folder(
    subfolder_path: Path, remote_to_local: bool = False
) -> str:
    """
    Compress e.g. `/path/archive` into `/path/archive.tar.gz`

    Note that `/path/archive.tar.gz` may already exist. In this case, it will
    be overwritten.

    Args:
        subfolder_path: Absolute path to the folder to compress.
        remote_to_local: If `True`, exclude some files from the tar.gz archive.

    Returns:
        Absolute path to the tar.gz archive.
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
        _copy_subfolder(
            subfolder_path, subfolder_path_tmp_copy, logger_name=logger_name
        )
        _create_tar_archive(
            tarfile_path,
            subfolder_path_tmp_copy,
            logger_name=logger_name,
            remote_to_local=remote_to_local,
        )
        return tarfile_path

    except Exception as e:
        logger.debug(f"ERROR: {e}")
        sys.exit(1)

    finally:
        _remove_temp_subfolder(
            subfolder_path_tmp_copy, logger_name=logger_name
        )


def main(sys_argv: list[str]):

    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder [--remote-to-local]\n"
    )
    num_args = len(sys_argv[1:])
    if num_args == 0:
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")
    elif num_args == 1:
        compress_folder(subfolder_path=Path(sys_argv[1]))
    elif num_args == 2 and sys_argv[2] == "--remote-to-local":
        compress_folder(subfolder_path=Path(sys_argv[1]), remote_to_local=True)
    else:
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")


if __name__ == "__main__":
    main(sys.argv)
