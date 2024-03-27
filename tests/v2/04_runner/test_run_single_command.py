import sys
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.executors.local import FractalThreadPoolExecutor
from fractal_server.app.runner.v2 import execute_tasks_v2
from fractal_server.app.runner.v2.models import Dataset
from fractal_server.app.runner.v2.models import Task
from fractal_server.app.runner.v2.models import WorkflowTask


@pytest.fixture()
def executor():
    with FractalThreadPoolExecutor() as e:
        yield e


def test_fractal_demos_01(tmp_path: Path, executor):

    # Define task
    task_path = Path(__file__).parent / "my_task.py"
    task_cmd = f"{sys.executable} {task_path.as_posix()}"
    task = Task(name="My Task", command_non_parallel=task_cmd)

    # Define workflowtask
    wftask = WorkflowTask(
        args_non_parallel=dict(image_dir="/tmp/input_images"),
        task=task,
        order=0,
        id=0,
    )

    # Define dataset
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = Dataset(zarr_dir=zarr_dir)

    # Execute Workflow
    dataset = execute_tasks_v2(
        wf_task_list=[wftask],
        dataset=dataset,
        executor=executor,
        workflow_dir=tmp_path,
    )

    debug(dataset)
