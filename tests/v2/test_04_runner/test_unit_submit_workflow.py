from devtools import debug

from fractal_server.app.models import UserSettings
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.v2.submit_workflow import submit_workflow
from fractal_server.app.schemas.v2 import JobStatusTypeV2


async def test_fail_submit_workflows_wrong_IDs(
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    job_factory_v2,
    tmp_path,
    db,
):
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
            user_settings=UserSettings(),
        )

        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
        )
        assert job.status == JobStatusTypeV2.SUBMITTED
        submit_workflow(
            workflow_id=9999999,
            dataset_id=9999999,
            job_id=job.id,
            user_id=user.id,
            user_settings=UserSettings(),
        )
        await db.refresh(job)
        assert job.status == JobStatusTypeV2.FAILED


async def test_fail_submit_workflows_wrong_backend(
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    task_factory_v2,
    tmp_path,
    db,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND="invalid")

    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
        )

        submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=job.id,
            user_id=user.id,
            user_settings=UserSettings(),
        )
        await db.refresh(job)
        assert "Invalid FRACTAL_RUNNER_BACKEND" in job.log


# FIXME: Re-introduce test
# async def test_mkdir_error(
#     project_factory_v2,
#     dataset_factory_v2,
#     workflow_factory_v2,
#     task_factory_v2,
#     job_factory_v2,
#     db,
#     tmp_path,
#     MockCurrentUser,
#     override_settings_factory,
# ):
#     override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")
#     async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:

#         project = await project_factory_v2(user)
#         dataset = await dataset_factory_v2(project_id=project.id, name="ds")
#         workflow = await workflow_factory_v2(project_id=project.id, name="wf")  # noqa
#         task = await task_factory_v2(user_id=user.id)
#         await _workflow_insert_task(
#             workflow_id=workflow.id, task_id=task.id, db=db
#         )
#         job = await job_factory_v2(
#             project_id=project.id,
#             dataset_id=dataset.id,
#             workflow_id=workflow.id,
#             working_dir=(tmp_path / "abc").as_posix(),
#             status="submitted",
#         )

#         submit_workflow(
#             workflow_id=workflow.id,
#             dataset_id=dataset.id,
#             job_id=job.id,
#             user_id=user.id,
#             user_cache_dir=(tmp_path / "xxx").as_posix(),
#             user_settings=UserSettings(),
#             slurm_user="fake",
#         )

#         await db.close()
#         job = await db.get(JobV2, job.id)

#         assert job.status == "failed"
#         assert job.log == (
#             "RuntimeError error occurred while creating job folder and "
#             "subfolders.\n"
#             "Original error: user=None not allowed in _mkdir_as_user"
#         )


async def test_submit_workflow_failure(
    tmp_path,
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    MockCurrentUser,
    db,
):
    """
    WHEN calling `submit_workflow`
    IF `working_dir` already exists
    THEN the job entry in the db is updated
    """

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
            working_dir_user=working_dir.as_posix(),
        )
        db.expunge_all()

        submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=job.id,
            user_id=user.id,
            user_settings=UserSettings(),
        )

    job = await db.get(JobV2, job.id)
    debug(job)
    assert job.status == "failed"
    assert "already exists" in job.log
