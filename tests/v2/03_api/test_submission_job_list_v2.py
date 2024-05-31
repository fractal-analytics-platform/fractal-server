from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    clean_app_job_list_v2,
)


async def test_clean_app_job_list_v2(
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    job_factory_v2,
    client,
    db,
    app,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_API_MAX_JOB_LIST_LENGTH=0)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        task = await task_factory_v2(source="source")
        project = await project_factory_v2(user)
        dataset1 = await dataset_factory_v2(project_id=project.id, name="ds-1")
        workflow1 = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        dataset2 = await dataset_factory_v2(project_id=project.id, name="ds-2")
        workflow2 = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )

        job1 = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow1.id,
            dataset_id=dataset1.id,
            status="submitted",
            working_dir="/somewhere",
        )
        job1_id = job1.id
        app.state.jobsV2.append(job1_id)

        res = await client.post(
            f"/api/v2/project/{project.id}/job/submit/"
            f"?workflow_id={workflow2.id}&dataset_id={dataset2.id}",
            json={},
        )
        assert res.status_code == 202
        job2_id = res.json()["id"]

        # Before clean-up, both jobs are listed
        assert app.state.jobsV2 == [job1_id, job2_id]
        # After clean-up, only the submitted job is left
        jobs_list = await clean_app_job_list_v2(db, app.state.jobsV2)
        assert jobs_list == [job1_id]
