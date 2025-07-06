from datetime import datetime

from fractal_server.app.models import HistoryRun
from fractal_server.app.models import JobV2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import JobStatusTypeV2


async def test_get_workflow_tasks_statuses(
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    MockCurrentUser,
    client,
    db,
):
    N_TASKS = 5
    assert N_TASKS > 2

    async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:
        task_group = TaskGroupV2(
            user_id=user.id,
            task_list=[
                TaskV2(
                    name=f"echo{i}",
                    type="non_parallel",
                    command_non_parallel=f"echo {i}",
                    args_schema_non_parallel={},
                    meta_non_parallel={},
                )
                for i in range(0, N_TASKS)
            ],
            origin="other",
            pkg_name="echoes",
            active=True,
        )

        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="dataset1"
        )
        workflow = await workflow_factory_v2(
            project_id=project.id, name="workflow"
        )

        wftask_ids = []
        for task in task_group.task_list:
            res = await client.post(
                f"/api/v2/project/{project.id}/workflow/{workflow.id}/wftask/"
                f"?task_id={task.id}",
                json={},
            )
            assert res.status_code == 201
            wftask_ids.append(res.json()["id"])

        old_job = JobV2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            user_email=user.email,
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=0,
            start_timestamp=datetime(2025, 7, 7, 7, 59, 59),
            end_timestamp=datetime(2025, 7, 7, 8, 59, 59),
            status=JobStatusTypeV2.DONE,
        )

        job = JobV2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            user_email=user.email,
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=1,
            last_task_index=3,
            start_timestamp=datetime(2025, 7, 7, 9, 59, 59),
        )
        db.add(old_job)
        db.add(job)
        await db.commit()
        await db.refresh(old_job)
        await db.refresh(job)

        db.add(
            HistoryRun(
                dataset_id=dataset.id,
                workflowtask_id=wftask_ids[0],
                job_id=old_job.id,
                task_id=task_group.task_list[0].id,
                workflowtask_dump={},
                task_group_dump={},
                status=HistoryUnitStatus.DONE,
                num_available_images=0,
                timestamp_started=datetime(2025, 7, 7, 8, 0, 0),
            )
        )

        db.add(
            HistoryRun(
                dataset_id=dataset.id,
                workflowtask_id=wftask_ids[1],
                job_id=job.id,
                task_id=task_group.task_list[1].id,
                workflowtask_dump={},
                task_group_dump={},
                status=HistoryUnitStatus.DONE,
                num_available_images=0,
                timestamp_started=datetime(2025, 7, 7, 10, 0, 1),
            )
        )

        db.add(
            HistoryRun(
                dataset_id=dataset.id,
                workflowtask_id=wftask_ids[2],
                job_id=job.id,
                task_id=task_group.task_list[2].id,
                workflowtask_dump={},
                task_group_dump={},
                status=HistoryUnitStatus.FAILED,
                num_available_images=0,
                timestamp_started=datetime(2025, 7, 7, 9, 0, 1),
            )
        )
        await db.commit()

        res = await client.get(
            f"api/v2/project/{project.id}/status/"
            f"?dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
        assert res.status_code == 200
        assert res.json()[str(task_group.task_list[0].id)]["status"] == (
            HistoryUnitStatus.DONE
        )
        assert res.json()[str(task_group.task_list[1].id)]["status"] == (
            HistoryUnitStatus.DONE
        )
        assert res.json()[str(task_group.task_list[2].id)]["status"] == (
            HistoryUnitStatus.SUBMITTED
        )
        assert res.json()[str(task_group.task_list[3].id)]["status"] == (
            HistoryUnitStatus.SUBMITTED
        )
        assert res.json()[str(task_group.task_list[4].id)] is None

        yet_another_running_job = JobV2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            user_email=user.email,
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=0,
        )
        db.add(yet_another_running_job)
        await db.commit()
        res = await client.get(
            f"api/v2/project/{project.id}/status/"
            f"?dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
        assert res.status_code == 422
        assert "Multiple running jobs found" in res.json()["detail"]
