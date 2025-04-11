from concurrent.futures import Executor
from pathlib import Path

import pytest

from .execute_tasks_v2 import execute_tasks_v2_mod
from fractal_server.app.runner.executors.local.runner import LocalRunner


@pytest.fixture()
def local_runner(tmp_path):
    root_dir_local = tmp_path / "job"
    with LocalRunner(root_dir_local=root_dir_local) as r:
        yield r


async def test_parallelize_on_no_images(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: Executor,
):
    """
    Run parallel&compound tasks on a dataset with no images.
    """
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, zarr_dir=zarr_dir
        )
        workflow = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(
            name="name",
            type="parallel",
            command_parallel="echo",
            user_id=user.id,
        )
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id,
            task_id=task.id,
            order=0,
        )
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job0",
            runner=local_runner,
            user_id=user.id,
        )

        task = await task_factory_v2(
            name="name",
            type="compound",
            command_non_parallel="echo",
            command_parallel="echo",
            user_id=user.id,
        )
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id,
            task_id=task.id,
            order=0,
        )
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job1",
            runner=local_runner,
            user_id=user.id,
        )
