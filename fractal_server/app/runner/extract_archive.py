import sys
import tarfile
from pathlib import Path

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
    with tarfile.open(tarfile_path) as tar:
        tar.extractall(path=job_folder)

    print(f"[extract_archive.py] {tarfile_path=}")
