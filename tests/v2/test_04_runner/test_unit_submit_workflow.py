from fractal_server.app.models import UserSettings
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.v2 import submit_workflow
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
