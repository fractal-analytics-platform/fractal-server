from concurrent.futures import Executor
from pathlib import Path

import pytest

from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.local.runner import LocalRunner

from .execute_tasks import execute_tasks_mod


@pytest.fixture()
def local_runner(
    tmp_path,
    local_resource_profile_objects,
):
    root_dir_local = tmp_path / "job"
    resource, profile = local_resource_profile_objects[:]
    with LocalRunner(
        root_dir_local=root_dir_local,
        resource=resource,
        profile=profile,
    ) as r:
        yield r


async def test_parallelize_on_no_images(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: Executor,
    local_resource_profile_db,
):
    """
    Run parallel&compound tasks on a dataset with no images.
    """
    # Preliminary setup
    resource, _ = local_resource_profile_db
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)

        task = await task_factory(
            name="name-1",
            type="parallel",
            command_parallel="echo",
            user_id=user.id,
        )
        wftask = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task.id,
            order=0,
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )
        with pytest.raises(JobExecutionError, match="empty image list"):
            execute_tasks_mod(
                wf_task_list=[wftask],
                dataset=dataset,
                workflow_dir_local=tmp_path / "job0",
                runner=local_runner,
                user_id=user.id,
                job_id=job.id,
                resource_id=resource.id,
            )

        task = await task_factory(
            name="name-2",
            type="compound",
            command_non_parallel="echo",
            command_parallel="echo",
            user_id=user.id,
        )
        wftask = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task.id,
            order=0,
        )
        with pytest.raises(JobExecutionError, match="empty image list"):
            execute_tasks_mod(
                wf_task_list=[wftask],
                dataset=dataset,
                workflow_dir_local=tmp_path / "job1",
                runner=local_runner,
                user_id=user.id,
                job_id=job.id,
                resource_id=resource.id,
            )
