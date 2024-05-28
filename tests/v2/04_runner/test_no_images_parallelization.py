import logging
from concurrent.futures import Executor
from pathlib import Path

from fixtures_mocks import *  # noqa: F401,F403
from v2_mock_models import DatasetV2Mock
from v2_mock_models import TaskV2Mock
from v2_mock_models import WorkflowTaskV2Mock


def execute_tasks_v2(wf_task_list, workflow_dir, **kwargs):
    from fractal_server.app.runner.task_files import task_subfolder_name
    from fractal_server.app.runner.v2.runner import (
        execute_tasks_v2 as raw_execute_tasks_v2,
    )

    for wftask in wf_task_list:
        if wftask.task is not None:
            subfolder = workflow_dir / task_subfolder_name(
                order=wftask.order, task_name=wftask.task.name
            )
        else:
            subfolder = workflow_dir / task_subfolder_name(
                order=wftask.order, task_name=wftask.task_legacy.name
            )
        logging.info(f"Now creating {subfolder.as_posix()}")
        subfolder.mkdir(parents=True)

    out = raw_execute_tasks_v2(
        wf_task_list=wf_task_list, workflow_dir=workflow_dir, **kwargs
    )
    return out


def test_parallelize_on_no_images(tmp_path: Path, executor: Executor):
    """
    Run a parallel task on a dataset with no images.
    """
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run successfully on an empty dataset
    execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=TaskV2Mock(
                    name="name",
                    type="parallel",
                    command_parallel="echo",
                    id=0,
                    source="source",
                ),
                id=0,
                order=0,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )


def test_parallelize_on_no_images_compound(tmp_path: Path, executor: Executor):
    """
    Run a compound task with an empty parallelization list.
    """
    # Preliminary setup
    execute_tasks_v2_args = dict(
        executor=executor,
        workflow_dir=tmp_path / "job_dir",
        workflow_dir_user=tmp_path / "job_dir",
    )
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run successfully on an empty dataset
    execute_tasks_v2(
        wf_task_list=[
            WorkflowTaskV2Mock(
                task=TaskV2Mock(
                    name="name",
                    type="compound",
                    # this produces an empty parallelization list
                    command_non_parallel="echo",
                    command_parallel="echo",
                    id=0,
                    source="source",
                ),
                id=0,
                order=0,
            )
        ],
        dataset=DatasetV2Mock(name="dataset", zarr_dir=zarr_dir),
        **execute_tasks_v2_args,
    )
