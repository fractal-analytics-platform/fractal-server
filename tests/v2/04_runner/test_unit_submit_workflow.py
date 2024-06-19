from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.v2 import submit_workflow


async def test_fail_submit_workflows_wrong_IDs(
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    db,
):
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2()
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        # Submitting an invalid job ID won't fail but will log an error
        await submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=9999999,
        )


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
        task = await task_factory_v2()
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
        )

        await submit_workflow(
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            job_id=job.id,
        )
        await db.refresh(job)
        assert "Invalid FRACTAL_RUNNER_BACKEND" in job.log
