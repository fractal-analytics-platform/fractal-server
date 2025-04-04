from devtools import debug

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)


async def test_status_legacy(
    MockCurrentUser,
    db,
    client,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
):

    async with MockCurrentUser() as user:

        task1 = await task_factory_v2(
            user_id=user.id, name="task1", command_non_parallel="echo"
        )
        task2 = await task_factory_v2(
            user_id=user.id, name="task2", command_non_parallel="echo"
        )
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        wftask1 = await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task1.id, db=db
        )
        wftask2 = await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task2.id, db=db
        )

        dataset1 = await dataset_factory_v2(
            project_id=project.id,
            name="ds1",
        )
        await client.get(
            f"/api/v2/project/{project.id}/status-legacy/"
            f"?dataset_id={dataset1.id}&workflow_id={workflow.id}"
        )

        dataset2 = await dataset_factory_v2(
            project_id=project.id,
            name="ds2",
            history=[
                {
                    "workflowtask": {
                        **wftask1.model_dump(),
                        "task": task1.model_dump(),
                    },
                    "status": "submitted",
                    "parallelization": {},
                }
            ],
        )
        await client.get(
            f"/api/v2/project/{project.id}/status-legacy/"
            f"?dataset_id={dataset2.id}&workflow_id={workflow.id}"
        )

        dataset2.history.append(
            {
                "workflowtask": {
                    **wftask2.model_dump(),
                    "task": task2.model_dump(),
                },
                "status": "done",
                "parallelization": {},
            }
        )

        from sqlalchemy.orm.attributes import flag_modified

        flag_modified(dataset2, "history")
        await db.commit()

        job = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset2.id,
            status="submitted",
            working_dir="/somewhere",
        )

        res = await client.get(
            f"/api/v2/project/{project.id}/status-legacy/"
            f"?dataset_id={dataset2.id}&workflow_id={workflow.id}"
        )
        debug(res.status_code, res.json())

        await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset2.id,
            status="submitted",
            working_dir="/somewhere",
        )
        res = await client.get(
            f"/api/v2/project/{project.id}/status-legacy/"
            f"?dataset_id={dataset2.id}&workflow_id={workflow.id}"
        )
        assert res.status_code == 422
        assert "linked to multiple active jobs" in res.json()["detail"]
