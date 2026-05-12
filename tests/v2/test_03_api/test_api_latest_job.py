import json

from devtools import debug

from fractal_server.app.models import JobV2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ResourceType

PREFIX = "/api/v2"
backends_available = list(element.value for element in ResourceType)


async def test_get_latest_jobs(
    db,
    client,
    project_factory,
    job_factory,
    workflow_factory,
    dataset_factory,
    task_factory,
    tmp_path,
    MockCurrentUser,
    local_resource_profile_db,
):
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        is_verified=True,
        profile_id=prof.id,
    ) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id, name="dataset")
        task = await task_factory(user_id=user.id)
        workflow = await workflow_factory(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db, order=0
        )

        await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="done",
        )
        job2 = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="submitted",
        )

        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json()["id"] == job2.id

        job3 = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="failed",
        )

        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json()["id"] == job3.id

        # 404

        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id + 1}&dataset_id={dataset.id}"
        )
        assert res.status_code == 404
        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id + 1}"
        )
        assert res.status_code == 404


async def test_get_latest_job_tasks_statuses(
    project_factory,
    dataset_factory,
    workflow_factory,
    MockCurrentUser,
    client,
    db,
    local_resource_profile_db,
):
    """
    Test the statuses returned by the latest-job endpoint, especially as of
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
    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        user_id = user.id
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id, name="dataset1")
        workflow = await workflow_factory(
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
            f"/api/v2/project/{project.id}/workflow/{workflow.id}/wftask/",
            json=[{"task_id": task.id}],
        )
        assert res.status_code == 201
        wftask_ids.append(res.json()[0]["id"])

    common_job_args = dict(
        project_id=project.id,
        workflow_id=workflow.id,
        dataset_id=dataset.id,
        user_email="",
        dataset_dump=json.loads(
            dataset.model_dump_json(exclude={"images", "history"})
        ),
        workflow_dump=json.loads(
            workflow.model_dump_json(
                exclude={"task_list", "description", "template_id"}
            )
        ),
        project_dump=json.loads(
            project.model_dump_json(exclude={"resource_id"})
        ),
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
        f"api/v2/project/{project.id}/latest-job/"
        f"?dataset_id={dataset.id}&workflow_id={workflow.id}"
    )
    assert res.status_code == 200
    assert res.json()["task_statuses"] == {
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


async def test_get_latest_job_image_counters(
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    workflowtask_factory,
    job_factory,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id)

        # WorkflowTask 1 (one run, four units, different statuses)
        wftask1 = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )
        run1 = HistoryRun(
            workflowtask_id=wftask1.id,
            dataset_id=dataset.id,
            workflowtask_dump={},
            task_group_dump={},
            num_available_images=3,
            status=HistoryUnitStatus.SUBMITTED,
            job_id=job.id,
        )
        db.add(run1)
        await db.commit()
        await db.refresh(run1)

        unit_a = HistoryUnit(
            history_run_id=run1.id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile="/log/a",
            zarr_urls=["/a"],
        )
        unit_b = HistoryUnit(
            history_run_id=run1.id,
            status=HistoryUnitStatus.DONE,
            logfile="/log/b",
            zarr_urls=["/b"],
        )
        unit_c = HistoryUnit(
            history_run_id=run1.id,
            status=HistoryUnitStatus.FAILED,
            logfile="/log/c",
            zarr_urls=["/c"],
        )
        db.add(unit_a)
        db.add(unit_b)
        db.add(unit_c)
        await db.commit()
        await db.refresh(unit_a)
        await db.refresh(unit_b)
        await db.refresh(unit_c)

        db.add(
            HistoryImageCache(
                zarr_url="/a",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                latest_history_unit_id=unit_a.id,
            )
        )
        db.add(
            HistoryImageCache(
                zarr_url="/b",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                latest_history_unit_id=unit_b.id,
            )
        )
        db.add(
            HistoryImageCache(
                zarr_url="/c",
                workflowtask_id=wftask1.id,
                dataset_id=dataset.id,
                latest_history_unit_id=unit_c.id,
            )
        )
        await db.commit()

        wftask2 = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )

        res = await client.get(
            f"/api/v2/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        debug(res.json())
        assert res.json()["task_statuses"] == {
            str(wftask1.id): {
                "status": "submitted",
                "num_available_images": 3,
                "num_done_images": 1,
                "num_submitted_images": 1,
                "num_failed_images": 1,
            },
            str(wftask2.id): None,
        }

        # Invalid `num_available_images`
        run1.num_available_images = 2
        db.add(run1)
        await db.commit()
        db.expunge_all()
        res = await client.get(
            f"/api/v2/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json()["task_statuses"] == {
            "1": {
                "status": "submitted",
                "num_available_images": None,
                "num_submitted_images": 1,
                "num_done_images": 1,
                "num_failed_images": 1,
            },
            "2": None,
        }
