import time

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    check_jobs_list_worker,
)


async def test_check_jobs_list_worker(
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    client,
    db,
    app,
    override_settings_factory,
    tmp_path,
):
    override_settings_factory(
        FRACTAL_API_SUBMIT_RATE_LIMIT=1,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp_path / "artifacts",
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, name="ds")
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(source="source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        res = await client.post(
            f"/api/v2/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202

        time.sleep(1)

        res = await client.post(
            f"/api/v2/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202

        jobs_list = await check_jobs_list_worker(db, app.state.jobs)
        # empty list because all jobs are failed
        assert jobs_list == []
