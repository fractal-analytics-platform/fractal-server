import sys

from devtools import debug

from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.runner.v2.submit_workflow import submit_workflow


async def test_fail_submit_workflows_wrong_IDs(
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    job_factory_v2,
    tmp_path,
    db,
    local_resource_profile_objects,
):
    res, prof = local_resource_profile_objects[:]
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=9999999,
            user_id=user.id,
            resource=res,
            profile=prof,
            user_cache_dir=tmp_path / "cache",
        )

        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
        )
        assert job.status == JobStatusType.SUBMITTED
        submit_workflow(
            workflow_id=9999999,
            dataset_id=9999999,
            job_id=job.id,
            user_id=user.id,
            resource=res,
            profile=prof,
            user_cache_dir=tmp_path / "cache",
        )
        await db.refresh(job)
        assert job.status == JobStatusType.FAILED


async def test_mkdir_error(
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    job_factory_v2,
    db,
    tmp_path,
    MockCurrentUser,
    slurm_sudo_resource_profile_objects,
):
    res, prof = slurm_sudo_resource_profile_objects[:]

    # Edit resource&profile so that we don't need to spin up containers to
    # reach the error branch
    res.jobs_slurm_python_worker = sys.executable
    prof.username = None

    async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, name="ds")
        workflow = await workflow_factory_v2(project_id=project.id, name="wf")  # noqa
        task = await task_factory_v2(user_id=user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=(tmp_path / "local").as_posix(),
            working_dir_user=(tmp_path / "remote").as_posix(),
            status="submitted",
        )

        submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=job.id,
            user_id=user.id,
            user_cache_dir=(tmp_path / "xxx").as_posix(),
            resource=res,
            profile=prof,
        )

        await db.close()
        job = await db.get(JobV2, job.id)

        assert job.status == "failed"
        assert "Could not mkdir" in job.log


async def test_submit_workflow_failure(
    tmp_path,
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    MockCurrentUser,
    db,
    local_resource_profile_objects,
):
    """
    WHEN calling `submit_workflow`
    IF `working_dir` already exists
    THEN the job entry in the db is updated
    """
    res, prof = local_resource_profile_objects[:]

    working_dir = tmp_path / "job_dir"
    working_dir.mkdir()
    assert working_dir.exists()

    async with MockCurrentUser() as user:
        task = await task_factory_v2(user_id=user.id)
        project = await project_factory_v2(user=user)
        workflow = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)
        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=working_dir.as_posix(),
            working_dir_user=(working_dir / "remote").as_posix(),
        )
        db.expunge_all()

        submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=job.id,
            user_id=user.id,
            resource=res,
            profile=prof,
            user_cache_dir=tmp_path / "cache",
        )

    job = await db.get(JobV2, job.id)
    debug(job)
    assert job.status == "failed"
    assert "FileExistsError" in job.log
