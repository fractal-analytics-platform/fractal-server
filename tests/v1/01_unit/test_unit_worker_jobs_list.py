import time

from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v1._aux_functions import (
    check_jobs_list_worker,
)
from fractal_server.app.runner.v1 import submit_workflow


async def test_success_submit_workflows(
    MockCurrentUser,
    db,
    app,
    project_factory,
    workflow_factory,
    dataset_factory,
    job_factory,
    resource_factory,
    task_factory,
    tmp_path,
):

    async with MockCurrentUser() as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="task_source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory(project_id=project.id)
        job0 = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            workflow_id=workflow.id,
        )
        await resource_factory(dataset)

        await submit_workflow(
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            job_id=job0.id,
        )

        time.sleep(1.01)

        await submit_workflow(
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            job_id=job0.id,
        )
        await db.close()

        jobs_list = await check_jobs_list_worker(db, app.state.jobsV1)
        # empty list because all jobs are failed
        assert jobs_list == []
