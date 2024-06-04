import sys
import tarfile
from pathlib import Path

if __name__ == "__main__":
    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder"
    )

    if len(sys.argv[1:]) != 1:
        raise ValueError(
            f"Invalid argument.\n{help_msg}\nProvided: {sys.argv=}"
        )

    subfolder_path = Path(sys.argv[1])
    print(f"[compress_folder.py] {subfolder_path=}")
    job_folder = subfolder_path.parent
    subfolder_name = subfolder_path.name
    tarfile_path = job_folder / f"{subfolder_name}.tar.gz"
    with tarfile.open(tarfile_path, "w:gz") as tar:
        tar.add(subfolder_path, arcname=subfolder_name, recursive=True)
    print(f"[compress_folder.py] {tarfile_path=}")
