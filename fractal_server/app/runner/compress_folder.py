import shlex
import shutil
import subprocess  # nosec
import sys
import time
from pathlib import Path


def run_subprocess(cmd):
    try:
        res = subprocess.run(  # nosec
            shlex.split(cmd), check=True, capture_output=True, encoding="utf-8"
        )
        return res
    except subprocess.CalledProcessError as e:
        print(
            f"Command '{e.cmd}' returned non-zero exit status {e.returncode}."
        )
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise e
    except Exception as e:
        print(f"An error occurred while running command: {cmd}")
        print(str(e))
        raise e


def copy_subfolder(src: Path, dest: Path):
    t0 = time.perf_counter()
    cmd_cp = f"cp -r {src.as_posix()} {dest.as_posix()}"
    res = run_subprocess(cmd_cp)
    t1 = time.perf_counter()
    print(f"[compress_folder.py] `cp -r` END - elapsed: {t1-t0:.3f} s")
    return res


def create_tar_archive(tarfile_path: Path, subfolder_path_tmp_copy: Path):
    cmd_tar = (
        f"tar czf {tarfile_path} "
        "--exclude *sbatch --exclude *_in_*.pickle "
        f"--directory={subfolder_path_tmp_copy.as_posix()} "
        "."
    )
    print(f"[compress_folder.py] cmd tar:\n{cmd_tar}")
    t0 = time.perf_counter()
    res = run_subprocess(cmd_tar)
    t1 = time.perf_counter()
    print(f"[compress_folder.py] tar END - elapsed: {t1-t0:.3f} s")
    return res


def remove_temp_subfolder(subfolder_path_tmp_copy: Path):
    t0 = time.perf_counter()
    try:
        shutil.rmtree(subfolder_path_tmp_copy)
    except Exception as e:
        print(f"[compress_folder.py] ERROR during shutil.rmtree: {e}")
    t1 = time.perf_counter()
    print(f"[compress_folder.py] shutil.rmtree END - elapsed: {t1-t0:.3f} s")


def compress_folder(subfolder_path: Path, tarfile_path: Path):
    subfolder_path_tmp_copy = (
        subfolder_path.parent / f"{subfolder_path.name}_copy"
    )

    t0 = time.perf_counter()

    try:
        copy_subfolder(subfolder_path, subfolder_path_tmp_copy)
        res = create_tar_archive(tarfile_path, subfolder_path_tmp_copy)
        if res.returncode != 0:
            raise Exception(f"Error in tar command: {res.stderr}")

    except Exception as e:
        print(f"[compress_folder.py] ERROR: {e}")
        shutil.rmtree(subfolder_path_tmp_copy)
        sys.exit(1)

    finally:
        remove_temp_subfolder(subfolder_path_tmp_copy)

    t1 = time.perf_counter()
    print(f"[compress_folder] END - elapsed {t1 - t0:.3f} seconds")


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
    print(f"[compress_folder.py] {subfolder_path=}")

    job_folder = subfolder_path.parent
    subfolder_name = subfolder_path.name
    tarfile_path = (job_folder / f"{subfolder_name}.tar.gz").as_posix()
    print(f"[compress_folder.py] {tarfile_path=}")
    compress_folder(subfolder_path=subfolder_path, tarfile_path=tarfile_path)
