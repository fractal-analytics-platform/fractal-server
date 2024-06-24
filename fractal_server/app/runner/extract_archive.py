import sys
import tarfile
from pathlib import Path


def _remove_suffix(*, string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[: -len(suffix)]
    else:
        raise ValueError(f"Cannot remove {suffix=} from {string=}.")


if __name__ == "__main__":
    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.extract_archive "
        "path/to/archive.tar.gz"
    )

    if len(sys.argv[1:]) != 1:
        raise ValueError(
            f"Invalid argument.\n{help_msg}\nProvided: {sys.argv=}"
        )
    elif not sys.argv[1].endswith(".tar.gz"):
        raise ValueError(
            f"Invalid argument.\n{help_msg}\nProvided: {sys.argv=}"
        )

    tarfile_path = Path(sys.argv[1])

    print(f"[extract_archive.py] {tarfile_path=}")

    job_folder = tarfile_path.parent
    subfolder_name = _remove_suffix(string=tarfile_path.name, suffix=".tar.gz")
    with tarfile.open(tarfile_path) as tar:
        tar.extractall(path=Path(job_folder, subfolder_name).as_posix())

    print(f"[extract_archive.py] {tarfile_path=}")
