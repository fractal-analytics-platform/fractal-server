from fractal_server.app.models import HistoryRun
from fractal_server.app.models import JobV2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import JobStatusType


async def test_get_workflow_tasks_statuses(
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    MockCurrentUser,
    client,
    db,
    local_resource_profile_db,
):
    """
    Test the status endpoint, especially as of
    https://github.com/fractal-analytics-platform/fractal-server/issues/2690


    WFTask | job_A | job_B               | expected status
    ------------------------------------------------------
    0      | DONE  | None                | DONE + counters
    1      | DONE  | DONE                | DONE + counters
    2      | DONE  | SUBMITTED (ongoing) | SUBMITTED + counters
    3      | DONE  | SUBMITTED           | SUBMITTED + counters
    4      | None  | SUBMITTED           | SUBMITTED (no counters)
    5      | None  | None                | None

    """
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs={"is_verified": True, "profile_id": profile.id}
    ) as user:
        user_id = user.id
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="dataset1"
        )
        workflow = await workflow_factory_v2(
            project_id=project.id, name="workflow"
        )

    task = TaskV2(
        name="echo",
        type="non_parallel",
        command_non_parallel="echo",
        args_schema_non_parallel={},
        meta_non_parallel={},
    )
    task_group = TaskGroupV2(
        user_id=user_id,
        task_list=[task],
        origin="other",
        pkg_name="echoes",
        active=True,
        resource_id=resource.id,
    )
    db.add(task_group)
    await db.commit()

    wftask_ids = []
    num_wftasks = 6
    for _ in range(0, num_wftasks):
        res = await client.post(
            f"/api/v2/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json={},
        )
        assert res.status_code == 201
        wftask_ids.append(res.json()["id"])

    common_job_args = dict(
        project_id=project.id,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        user_email="",
        dataset_dump={},
        workflow_dump={},
        project_dump={},
    )
    job_A = JobV2(
        first_task_index=0,
        last_task_index=3,
        status=JobStatusType.DONE,
        **common_job_args,
    )
    job_B = JobV2(
        first_task_index=1,
        last_task_index=4,
        status=JobStatusType.SUBMITTED,
        **common_job_args,
    )
    db.add_all([job_A, job_B])
    await db.commit()
    await db.refresh(job_A)
    await db.refresh(job_B)

    common_run_args = dict(
        dataset_id=dataset.id,
        task_id=task.id,
        workflowtask_dump={},
        task_group_dump={},
        num_available_images=0,
    )

    # Job_A's HistoryRuns
    db.add_all(
        [
            HistoryRun(
                workflowtask_id=wftask_ids[ind],
                job_id=job_A.id,
                status=HistoryUnitStatus.DONE,
                **common_run_args,
            )
            for ind in [0, 1, 2, 3]
        ]
    )
    await db.commit()

    # Job_B's HistoryRuns
    db.add_all(
        [
            HistoryRun(
                workflowtask_id=wftask_ids[1],
                job_id=job_B.id,
                status=HistoryUnitStatus.DONE,
                **common_run_args,
            ),
            HistoryRun(
                workflowtask_id=wftask_ids[2],
                job_id=job_B.id,
                status=HistoryUnitStatus.SUBMITTED,
                **common_run_args,
            ),
        ]
    )
    await db.commit()

    res = await client.get(
        f"api/v2/project/{project.id}/status/"
        f"?dataset_id={dataset.id}&workflow_id={workflow.id}"
    )
    assert res.status_code == 200
    assert res.json() == {
        str(wftask_ids[0]): {
            "status": "done",
            "num_available_images": 0,
            "num_submitted_images": 0,
            "num_done_images": 0,
            "num_failed_images": 0,
        },
        str(wftask_ids[1]): {
            "status": "done",
            "num_available_images": 0,
            "num_submitted_images": 0,
            "num_done_images": 0,
            "num_failed_images": 0,
        },
        str(wftask_ids[2]): {
            "status": "submitted",
            "num_available_images": 0,
            "num_submitted_images": 0,
            "num_done_images": 0,
            "num_failed_images": 0,
        },
        str(wftask_ids[3]): {
            "status": "submitted",
            "num_available_images": 0,
            "num_submitted_images": 0,
            "num_done_images": 0,
            "num_failed_images": 0,
        },
        str(wftask_ids[4]): {"status": "submitted"},
        str(wftask_ids[5]): None,
    }


async def test_multiple_jobs_error(
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    MockCurrentUser,
    client,
    db,
):
    """
    Test the 422 response for the (in principle unreachable) case of two
    simultaneouly-submitted jobs for the same dataset/workflow pair.
    """

    async with MockCurrentUser(user_kwargs={"is_verified": True}) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="dataset1"
        )
        workflow = await workflow_factory_v2(
            project_id=project.id, name="workflow"
        )

    # UnreachableBranchError
    for ind in [0, 1]:
        job = JobV2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            user_email="",
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=0,
            status=JobStatusType.SUBMITTED,
        )
        db.add(job)
        await db.commit()

    res = await client.get(
        f"api/v2/project/{project.id}/status/"
        f"?dataset_id={dataset.id}&workflow_id={workflow.id}"
    )
    assert res.status_code == 422
    assert "Multiple running jobs found" in res.json()["detail"]
