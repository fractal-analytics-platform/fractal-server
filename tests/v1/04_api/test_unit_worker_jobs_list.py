from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v1._aux_functions import (
    clean_app_job_list_v1,
)


async def test_clean_app_job_list_v1(
    MockCurrentUser,
    db,
    app,
    client,
    project_factory,
    workflow_factory,
    dataset_factory,
    resource_factory,
    job_factory,
    task_factory,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_API_MAX_JOB_LIST_LENGTH=0)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        task = await task_factory(name="task", source="task_source")
        project = await project_factory(user)
        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )
        dataset1_in = await dataset_factory(project_id=project.id, type="zarr")
        dataset1_out = await dataset_factory(
            project_id=project.id, type="zarr"
        )
        dataset2_in = await dataset_factory(project_id=project.id, type="zarr")
        dataset2_out = await dataset_factory(
            project_id=project.id, type="zarr"
        )
        await resource_factory(dataset1_in)
        await resource_factory(dataset1_out)
        await resource_factory(dataset2_in)
        await resource_factory(dataset2_out)

        job1 = await job_factory(
            project_id=project.id,
            workflow_id=workflow1.id,
            input_dataset_id=dataset1_in.id,
            output_dataset_id=dataset1_out.id,
            status="submitted",
            working_dir="/somewhere",
        )
        job1_id = job1.id
        app.state.jobsV2.append(job1_id)

        res = await client.post(
            f"/api/v1/project/{project.id}/workflow/{workflow2.id}/apply/"
            f"?input_dataset_id={dataset2_in.id}"
            f"&output_dataset_id={dataset2_out.id}",
            json={},
        )
        assert res.status_code == 202
        job2_id = res.json()["id"]

        # Before clean-up, both jobs are listed
        assert app.state.jobsV2 == [job1_id, job2_id]
        # After clean-up, only the submitted job is left
        jobs_list = await clean_app_job_list_v1(db, app.state.jobsV1)
        assert jobs_list == [job1_id]
