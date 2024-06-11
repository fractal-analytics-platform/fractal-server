import sys
import tarfile
import time
from pathlib import Path
from typing import Optional


def _filter(info: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
    # print(f"START FILTERING {info.name}")
    if info.name.endswith(".pickle"):
        filename = info.name.split("/")[-1]
        parts = filename.split("_")
        if len(parts) == 3 and parts[1] == "in":
            print(f"SKIP {info.name=}")
            return None
        elif len(parts) == 5 and parts[3] == "in":
            print(f"SKIP {info.name=}")
            return None
    elif info.name.endswith(".args.json"):
        print(f"SKIP {info.name=}")
        return None
    elif info.name.endswith("slurm_submit.sbatch"):
        print(f"SKIP {info.name=}")
        return None
    print(f"OK {info.name=}")
    return info


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
    t_0 = time.perf_counter()
    print("[compress_folder.py] START")
    print(f"[compress_folder.py] {subfolder_path=}")
    job_folder = subfolder_path.parent
    subfolder_name = subfolder_path.name
    tarfile_path = job_folder / f"{subfolder_name}.tar.gz"
    with tarfile.open(tarfile_path, "w:gz") as tar:
        tar.add(
            subfolder_path,
            arcname=subfolder_name,
            recursive=True,
            filter=_filter,
        )
    print(f"[compress_folder.py] {tarfile_path=}")
    t_1 = time.perf_counter()
    print(f"[compress_folder] END - elapsed {t_1 - t_0:.3f} seconds")
