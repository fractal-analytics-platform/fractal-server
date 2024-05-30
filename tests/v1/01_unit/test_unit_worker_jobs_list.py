from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v1._aux_functions import (
    clean_app_job_list_v1,
)


async def test_success_submit_workflows(
    MockCurrentUser,
    db,
    app,
    client,
    project_factory,
    workflow_factory,
    dataset_factory,
    resource_factory,
    task_factory,
):
    user_kwargs = {"is_verified": True}

    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="task_source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset_in = await dataset_factory(project_id=project.id, type="zarr")
        dataset_out = await dataset_factory(project_id=project.id, type="zarr")

        await resource_factory(
            dataset_in,
        )
        await resource_factory(dataset_out)

        # EXECUTE WORKFLOW
        res = await client.post(
            f"/api/v1/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset_in.id}"
            f"&output_dataset_id={dataset_out.id}",
            json={},
        )
        assert res.status_code == 202

        jobs_list = await clean_app_job_list_v1(db, app.state.jobsV1)
        # empty list because all jobs are failed
        assert jobs_list == []
