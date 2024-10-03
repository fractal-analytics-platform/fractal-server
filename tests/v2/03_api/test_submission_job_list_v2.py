from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    clean_app_job_list_v2,
)


async def test_clean_app_job_list_v2(
    MockCurrentUser,
    db,
    app,
    client,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    override_settings_factory,
):

    # Check that app fixture starts in a clean state
    assert app.state.jobsV1 == []
    assert app.state.jobsV2 == []

    # Set this to 0 so that the endpoint also calls the clean-up function
    override_settings_factory(FRACTAL_API_MAX_JOB_LIST_LENGTH=0)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        # Create DB objects
        task = await task_factory_v2(
            user_id=user.id, name="task", command="echo"
        )
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset1 = await dataset_factory_v2(project_id=project.id, name="ds-1")
        dataset2 = await dataset_factory_v2(project_id=project.id, name="ds-2")

        # Create job with submitted status
        job1 = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset1.id,
            status="submitted",
            working_dir="/somewhere",
        )
        job1_id = job1.id
        app.state.jobsV2.append(job1_id)

        # Submit a second job via API
        res = await client.post(
            f"/api/v2/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset2.id}",
            json={},
        )
        assert res.status_code == 202
        job2_id = res.json()["id"]

        # Before clean-up, both jobs are listed
        assert app.state.jobsV2 == [job1_id, job2_id]

        # After clean-up, only the submitted job is left
        jobs_list = await clean_app_job_list_v2(db, app.state.jobsV2)
        assert jobs_list == [job1_id]
