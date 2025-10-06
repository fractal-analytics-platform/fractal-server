import cProfile
import json
import os
import pstats
import shlex
import shutil
import subprocess  # nosec
import sys
import tempfile
from pathlib import Path

import benchmarks.runner
import tests.v2.fractal_tasks_mock.dist as dist
from benchmarks.runner.mocks import DatasetV2Mock
from benchmarks.runner.mocks import TaskV2Mock
from benchmarks.runner.mocks import WorkflowTaskV2Mock
from fractal_server.runner.v2._local import LocalRunner
from fractal_server.runner.v2.runner import execute_tasks_v2


def _run_cmd(cmd: str) -> str:
    res = subprocess.run(  # nosec
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if not res.returncode == 0:
        raise ValueError(res)
    return res.stdout


def mock_venv(tmp_path: str) -> dict:
    venv = f"{tmp_path}/venv"
    python = f"{venv}/bin/python"

    if not os.path.isdir(venv):
        # Create venv
        _run_cmd(f"{sys.executable} -m venv {tmp_path}/venv")
        _run_cmd(
            f"{python} -m pip install "
            f"{dist.__path__[0]}/fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )

    # Extract installed-package folder
    out = _run_cmd(f"{python} -m pip show fractal_tasks_mock")
    location = next(
        line for line in out.split("\n") if line.startswith("Location:")
    )
    location = location.replace("Location: ", "")
    src_dir = Path(location) / "fractal_tasks_mock/"

    with (src_dir / "__FRACTAL_MANIFEST__.json").open("r") as f:
        manifest = json.load(f)

    task_dict = {}
    for ind, task in enumerate(manifest["task_list"]):
        args = {}
        if task.get("executable_non_parallel"):
            args[
                "command_non_parallel"
            ] = f"{python} {src_dir / task['executable_non_parallel']}"
            args["meta_non_parallel"] = task.get("meta_non_parallel")
        if task.get("executable_parallel"):
            args[
                "command_parallel"
            ] = f"{python} {src_dir / task['executable_parallel']}"
            args["meta_parallel"] = task.get("meta_parallel")

        t = TaskV2Mock(
            id=ind,
            name=task["name"],
            source=task["name"].replace(" ", "_"),
            input_types=task.get("input_types", {}),
            output_types=task.get("output_types", {}),
            **args,
        )
        task_dict[t.name] = t

    return task_dict


venv_dir = tempfile.mkdtemp()
fractal_tasks_mock_venv = mock_venv(venv_dir)


def benchmark(N: int, tmp_path: str):
    WORKING_DIR = Path(f"{tmp_path}/job")
    ZARR_DIR = Path(f"{tmp_path}/zarr").as_posix().rstrip("/")
    execute_tasks_v2(
        wf_task_list=[
            # compound
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir=ZARR_DIR, num_images=N),
                id=0,
                order=0,
            ),
            # parallel
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["illumination_correction"],
                args_parallel=dict(overwrite_input=True),
                id=1,
                order=1,
            ),
            # compound
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["MIP_compound"],
                id=2,
                order=2,
            ),
            # parallel
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["cellpose_segmentation"],
                id=3,
                order=3,
            ),
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=ZARR_DIR),
        workflow_dir_local=WORKING_DIR,
        workflow_dir_remote=WORKING_DIR,
        runner=LocalRunner(),
    )


if __name__ == "__main__":
    results = []

    for N in [100, 200, 300]:
        tmp_path = tempfile.mkdtemp()
        cProfile.run(f"benchmark({N}, '{tmp_path}')", "profile_results")
        stats = pstats.Stats("profile_results")
        stats.sort_stats("tottime")

        thread_time = stats.stats[
            "~", 0, "<method 'acquire' of '_thread.lock' objects>"
        ][2]
        total_time = stats.total_tt

        list_dirs = {}
        for path, key in [
            (f"{tmp_path}/job", "job_dir"),
            (f"{tmp_path}/zarr", "zarr_dir"),
        ]:
            size = 0
            count = 0
            for file in os.listdir(path):
                if os.path.isfile(f"{path}/{file}"):
                    count += 1
                    size += os.path.getsize(f"{path}/{file}")
            list_dirs[key] = dict(count=count, size=f"{size / 1024:.2f} KB")

        shutil.rmtree(tmp_path)

        results.append(
            dict(
                N=N,
                thread_time=thread_time,
                total_time=total_time,
                list_dirs=list_dirs,
            )
        )

    runner = os.path.dirname(benchmarks.runner.__path__[0])
    with open(f"{runner}/runner/runner_benchmark.txt", "w") as file:
        # Headers
        to_write = (
            "\n\n\n"
            "Parallel Tasks: 2\n"
            "Compound Tasks: 2\n"
            "Images: N\n"
            "\n"
            "|\tN\t"
            "|\texecutor\t"
            "|\ttotal\t"
            "|\toverhead\t"
            "|\tjob_dir\t\t\t"
            "|\n"
            "|\t---\t|\t---\t|\t---\t|\t---\t\t|\t---\t\t\t|"
            "\n"
        )
        # Results
        for result in results:
            to_write += (
                f"|\t{result['N']}\t"
                f"|\t{result['thread_time']:.4f}\t"
                f"|\t{result['total_time']:.4f}\t"
                f"|\t{(result['total_time'] - result['thread_time']):.4f}\t\t"
                f"|\t{tuple(result['list_dirs']['job_dir'].values())}\t"
                "\n"
            )
        print(to_write)
        file.write(to_write)
