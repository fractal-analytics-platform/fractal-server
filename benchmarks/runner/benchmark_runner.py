import json
import os
import shlex
import shutil
import subprocess  # nosec
import sys
import tempfile
from pathlib import Path
from time import perf_counter

import tests.v2.fractal_tasks_mock.dist as dist
from benchmarks.runner.mocks import DatasetV2Mock
from benchmarks.runner.mocks import TaskV2Mock
from benchmarks.runner.mocks import WorkflowTaskV2Mock
from fractal_server.app.runner.v2._local import FractalThreadPoolExecutor
from fractal_server.app.runner.v2.runner import execute_tasks_v2


def _run_cmd(cmd: str) -> str:
    res = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if not res.returncode == 0:
        raise ValueError(res)
    return res.stdout


# executor: Executor, fractal_tasks_mock_venv,


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
            args["meta_non_paralell"] = task.get("meta_non_paralell")
        if task.get("executable_parallel"):
            args[
                "command_parallel"
            ] = f"{python} {src_dir / task['executable_parallel']}"
            args["meta_paralell"] = task.get("meta_paralell")

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


def benchmark(N: int):

    tmp_path = tempfile.mkdtemp()
    WORKING_DIR = Path(f"{tmp_path}/job_dir")
    ZARR_DIR = (WORKING_DIR / "zarr").as_posix().rstrip("/")

    fractal_tasks_mock_venv = mock_venv(tmp_path)

    start = perf_counter()
    execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir=ZARR_DIR, num_images=N),
                id=0,
                order=0,
            ),
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["illumination_correction"],
                args_parallel=dict(overwrite_input=True),
                id=1,
                order=1,
            ),
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["MIP_compound"],
                id=2,
                order=2,
            ),
            WorkflowTaskV2Mock(
                task=fractal_tasks_mock_venv["cellpose_segmentation"],
                id=3,
                order=3,
            ),
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=ZARR_DIR),
        workflow_dir=WORKING_DIR,
        workflow_dir_user=WORKING_DIR,
        executor=FractalThreadPoolExecutor(),
    )
    stop = perf_counter()

    count = 0
    size = 0
    for file in os.listdir(WORKING_DIR):
        if os.path.isfile(WORKING_DIR / file):
            count += 1
            size += os.path.getsize(WORKING_DIR / file)

    shutil.rmtree(tmp_path)

    return dict(N=N, count=count, size=size, time=stop - start)


if __name__ == "__main__":

    results = []
    for N in [10, 100, 1000]:
        results.append(benchmark(N))

    keys = ["N", "count", "size", "time"]

    for key in keys:
        print(f"{key}\t", end="")
    print()

    for result in results:
        for key in keys:
            print(f"{result[key]}\t", end="")
        print()
