import shlex
import subprocess  # nosec
import sys
import tarfile
import time
from pathlib import Path
from typing import Optional


# COMPRESS_FOLDER_MODALITY = "python"
COMPRESS_FOLDER_MODALITY = "cp-tar-rmtree"


def _filter(info: tarfile.TarInfo) -> Optional[tarfile.TarInfo]:
    if info.name.endswith(".pickle"):
        filename = info.name.split("/")[-1]
        parts = filename.split("_")
        if len(parts) == 3 and parts[1] == "in":
            return None
        elif len(parts) == 5 and parts[3] == "in":
            return None
    elif info.name.endswith("slurm_submit.sbatch"):
        return None
    return info


if __name__ == "__main__":
    help_msg = (
        "Expected use:\n"
        "python -m fractal_server.app.runner.compress_folder "
        "path/to/folder"
    )

    if len(sys.argv[1:]) != 1:
        raise ValueError(
            "Invalid argument(s).\n" f"{help_msg}\n" f"Provided: {sys.argv=}"
        )

    subfolder_path = Path(sys.argv[1])
    t_0 = time.perf_counter()
    print("[compress_folder.py] START")
    print(f"[compress_folder.py] {COMPRESS_FOLDER_MODALITY=}")
    print(f"[compress_folder.py] {subfolder_path=}")

    job_folder = subfolder_path.parent
    subfolder_name = subfolder_path.name
    tarfile_path = (job_folder / f"{subfolder_name}.tar.gz").as_posix()
    print(f"[compress_folder.py] {tarfile_path=}")

    if COMPRESS_FOLDER_MODALITY == "python":
        raise NotImplementedError()
        with tarfile.open(tarfile_path, "w:gz") as tar:
            tar.add(
                subfolder_path,
                arcname=".",  # ????
                recursive=True,
                filter=_filter,
            )
    elif COMPRESS_FOLDER_MODALITY == "cp-tar-rmtree":
        import shutil
        import time

        subfolder_path_tmp_copy = (
            subfolder_path.parent / f"{subfolder_path.name}_copy"
        )

        t0 = time.perf_counter()
        # shutil.copytree(subfolder_path, subfolder_path_tmp_copy)
        cmd_cp = (
            "cp -r "
            f"{subfolder_path.as_posix()} "
            f"{subfolder_path_tmp_copy.as_posix()}"
        )
        res = subprocess.run(  # nosec
            shlex.split(cmd_cp),
            check=True,
            capture_output=True,
            encoding="utf-8",
        )
        t1 = time.perf_counter()
        print("[compress_folder.py] `cp -r` END - " f"elapsed: {t1-t0:.3f} s")

        cmd_tar = (
            "tar czf "
            f"{tarfile_path} "
            "--exclude *sbatch --exclude *_in_*.pickle "
            f"--directory={subfolder_path_tmp_copy.as_posix()} "
            "."
        )

        print(f"[compress_folder.py] cmd tar:\n{cmd_tar}")
        t0 = time.perf_counter()
        res = subprocess.run(  # nosec
            shlex.split(cmd_tar),
            capture_output=True,
            encoding="utf-8",
        )
        t1 = time.perf_counter()
        t_1 = time.perf_counter()
        print(f"[compress_folder.py] tar END - elapsed: {t1-t0:.3f} s")

        print(f"[compress_folder] END - elapsed {t_1 - t_0:.3f} seconds")

        if res.returncode != 0:
            print("[compress_folder.py] ERROR in tar")
            print(f"[compress_folder.py] tar stdout:\n{res.stdout}")
            print(f"[compress_folder.py] tar stderr:\n{res.stderr}")

            shutil.rmtree(subfolder_path_tmp_copy)
            sys.exit(1)

        t0 = time.perf_counter()
        shutil.rmtree(subfolder_path_tmp_copy)
        t1 = time.perf_counter()
        print(
            f"[compress_folder.py] shutil.rmtree END - elapsed: {t1-t0:.3f} s"
        )

    t_1 = time.perf_counter()
    print(f"[compress_folder] END - elapsed {t_1 - t_0:.3f} seconds")
