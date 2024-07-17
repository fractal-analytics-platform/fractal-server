"""
Wrap `tar` extraction command.

This module is used both locally (in the environment where `fractal-server`
is running) and remotely (as a standalon Python module, executed over SSH).

This is a twin-module of `compress_folder.py`.

The reason for using the `tar` command via `subprocess` rather than Python
built-in `tarfile` library has to do with performance issues we observed
when handling files which were just created within a SLURM job, and in the
context of a CephFS filesystem.
"""
import sys
from pathlib import Path

from .run_subprocess import run_subprocess
from fractal_server.logger import set_logger


def _remove_suffix(*, string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        raise ValueError(f"Cannot remove {suffix=} from {string=}.")


def extract_archive(archive_path: Path):
    """
    Extract e.g. `/path/archive.tar.gz` archive into `/path/archive` folder

    Note that `/path/archive` may already exist. In this case, files with
    the same name are overwritten and new files are added.

    Arguments:
        archive_path: Absolute path to the archive file.
    """

    logger_name = "extract_archive"
    logger = set_logger(logger_name)

    logger.debug("START")
    logger.debug(f"{archive_path.as_posix()=}")

    # Check archive_path is valid
    if not archive_path.exists():
        sys.exit(f"Missing file {archive_path.as_posix()}.")

    # Prepare subfolder path
    parent_dir = archive_path.parent
    subfolder_name = _remove_suffix(string=archive_path.name, suffix=".tar.gz")
    subfolder_path = parent_dir / subfolder_name
    logger.debug(f"{subfolder_path.as_posix()=}")

    # Create subfolder
    subfolder_path.mkdir(exist_ok=True)

    # Run tar command
    cmd_tar = (
        f"tar -xzvf {archive_path} "
        f"--directory={subfolder_path.as_posix()} "
        "."
    )
    logger.debug(f"{cmd_tar=}")
    run_subprocess(cmd=cmd_tar, logger_name=logger_name)

    logger.debug("END")


def main(sys_argv: list[str]):
    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.extract_archive "
        "path/to/archive.tar.gz"
    )

    if len(sys_argv[1:]) != 1 or not sys_argv[1].endswith(".tar.gz"):
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")
    else:
        tarfile_path = Path(sys_argv[1])
        extract_archive(tarfile_path)


if __name__ == "__main__":
    main(sys.argv)
