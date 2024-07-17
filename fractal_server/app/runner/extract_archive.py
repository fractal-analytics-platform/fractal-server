import sys
import tarfile
from pathlib import Path

from fractal_server.logger import set_logger


def _remove_suffix(*, string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        raise ValueError(f"Cannot remove {suffix=} from {string=}.")


def extract_archive(archive_path: Path):
    """
    Extract a `/path/archive.tar.gz` archive into `/path/archive` folder

    Given archive_path="/tmp/asd/asd.tar.gz
    HANDLE invalid paths
    Extract it to /tmp/asd/asd + HANDLE /tmp/asd/asd already present

    if folder exists, do not remove it and just add new files


    Arguments:
        archive_path: Absolute path to the archive file.
    """

    logger = set_logger("extract_archive")

    logger.info("START")
    logger.info(f"{archive_path=}")

    parent_dir = archive_path.parent
    logger.info(f"{parent_dir=}")

    subfolder_name = _remove_suffix(string=archive_path.name, suffix=".tar.gz")
    logger.info(f"{subfolder_name=}")

    with tarfile.open(archive_path) as tar:
        tar.extractall(path=Path(parent_dir, subfolder_name).as_posix())

    logger.info("END")


def main(sys_argv: list[str]):
    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.extract_archive "
        "path/to/archive.tar.gz"
    )

    if len(sys_argv[1:]) != 1 or not sys_argv[1].endswith(".tar.gz"):
        sys.exit(f"Invalid argument.\n{help_msg}\nProvided: {sys_argv[1:]=}")

    tarfile_path = Path(sys.argv[1])
    extract_archive(tarfile_path)


if __name__ == "__main__":
    main(sys.argv)
