from sqlalchemy.orm.attributes import flag_modified

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)


async def test_status_legacy(
    MockCurrentUser,
    db,
    client,
    task_factory,
    project_factory,
    workflow_factory,
    dataset_factory,
    job_factory,
):
    async with MockCurrentUser() as user:
        task1 = await task_factory(
            user_id=user.id, name="task1", command_non_parallel="echo"
        )
        task2 = await task_factory(
            user_id=user.id, name="task2", command_non_parallel="echo"
        )
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        wftask1 = await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task1.id, db=db
        )
        wftask2 = await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task2.id, db=db
        )

        dataset1 = await dataset_factory(
            project_id=project.id,
            name="ds1",
        )
        res = await client.get(
            f"/api/v2/project/{project.id}/status-legacy/"
            f"?dataset_id={dataset1.id}&workflow_id={workflow.id}"
        )
        assert res.status_code == 200
        assert res.json() == {"status": {}}

        dataset2 = await dataset_factory(
            project_id=project.id,
            name="ds2",
            history=[
                {
                    "workflowtask": {
                        **wftask1.model_dump(),
                        "task": task1.model_dump(),
                    },
                    "status": "failed",
                    "parallelization": {},
                }
            ],
        )
        res = await client.get(
            f"/api/v2/project/{project.id}/status-legacy/"
            f"?dataset_id={dataset2.id}&workflow_id={workflow.id}"
        )
        assert res.status_code == 200
        assert res.json() == {"status": {str(wftask1.id): "failed"}}

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

        flag_modified(dataset2, "history")
        await db.commit()

        await job_factory(
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
        assert res.status_code == 200
        assert res.json() == {
            "status": {
                str(wftask1.id): "submitted",
                str(wftask2.id): "submitted",
            }
        }
