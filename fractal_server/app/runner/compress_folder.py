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
from fractal_server.logger import set_logger


def compress_folder(
    subfolder_path: Path,
    filelist_path: str | None,
    default_logging_level: int | None = None,
) -> str:
    """
    Compress e.g. `/path/archive` into `/path/archive.tar.gz`

    Note that `/path/archive.tar.gz` may already exist. In this case, it will
    be overwritten.

    Args:
        subfolder_path: Absolute path to the folder to compress.
        remote_to_local: If `True`, exclude some files from the tar.gz archive.
        default_logging_level:

    Returns:
        Absolute path to the tar.gz archive.
    """

    # Assign an almost-unique label to the logger name, to simplify grepping
    # logs when several `compress_folder` functions are run concurrently
    label = round(time.time(), 2)
    logger_name = f"compress_folder_{label}"
    logger = set_logger(
        logger_name,
        default_logging_level=default_logging_level,
    )
    logger.debug("START")
    t_start = time.perf_counter()

    subfolder_name = subfolder_path.name
    tarfile_path = (
        subfolder_path.parent / f"{subfolder_name}.tar.gz"
    ).as_posix()

    logger.debug(f"{subfolder_path=}")
    logger.debug(f"{tarfile_path=}")

    if filelist_path is None:
        cmd_tar = (
            f"tar -c -z -f {tarfile_path} "
            f"--directory={subfolder_path.as_posix()} "
            "."
        )
    else:
        cmd_tar = (
            f"tar -c -z -f {tarfile_path} "
            f"--directory={subfolder_path.as_posix()} "
            f"--files-from={filelist_path} --ignore-failed-read"
        )
    logger.debug(f"{cmd_tar=}")

    try:
        run_subprocess(cmd=cmd_tar, logger_name=logger_name)
        elapsed = time.perf_counter() - t_start
        logger.debug(f"END {elapsed=} s ({tarfile_path})")
        return tarfile_path
    except Exception as e:
        logger.debug(f"ERROR: {str(e)}")
        cmd_rm = f"rm {tarfile_path}"
        try:
            run_subprocess(cmd=cmd_rm, logger_name=logger_name)
        except Exception as e_rm:
            logger.error(
                f"Running {cmd_rm=} failed, original error: {str(e_rm)}."
            )

        sys.exit(1)


def main(
    sys_argv: list[str],
    default_logging_level: int | None = None,
):

    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder [--filelist /path/to/filelist]\n"
    )
    num_args = len(sys_argv[1:])
    if num_args == 0:
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")
    elif num_args == 1:
        compress_folder(
            subfolder_path=Path(sys_argv[1]),
            filelist_path=None,
            default_logging_level=default_logging_level,
        )
    elif num_args == 3 and sys_argv[2] == "--filelist":
        compress_folder(
            subfolder_path=Path(sys_argv[1]),
            filelist_path=sys_argv[3],
            default_logging_level=default_logging_level,
        )
    else:
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")


if __name__ == "__main__":
    import logging

    main(sys.argv, default_logging_level=logging.DEBUG)
