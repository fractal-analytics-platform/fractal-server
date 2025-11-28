from datetime import datetime
from typing import Literal

import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import HistoryUnitStatusWithUnset
from fractal_server.images import SingleImage
from fractal_server.images.status_tools import IMAGE_STATUS_KEY


async def test_status_api(
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
            f"/api/v2/project/{project.id}/status/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        debug(res.json())
        assert res.json() == {
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
            f"/api/v2/project/{project.id}/status/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json() == {
            "1": {
                "status": "submitted",
                "num_available_images": None,
                "num_submitted_images": 1,
                "num_done_images": 1,
                "num_failed_images": 1,
            },
            "2": None,
        }


@pytest.mark.parametrize(
    "object_to_delete",
    [
        "project",
        "dataset",
        "workflow",
        "workflowtask",
    ],
)
async def test_cascade_delete(
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    workflowtask_factory,
    job_factory,
    db,
    client,
    MockCurrentUser,
    object_to_delete: Literal[
        "project",
        "dataset",
        "workflow",
        "workflowtask",
    ],
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id)
        wftask = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )
        run = HistoryRun(
            workflowtask_id=wftask.id,
            dataset_id=dataset.id,
            workflowtask_dump={},
            task_group_dump={},
            num_available_images=3,
            status=HistoryUnitStatus.SUBMITTED,
            job_id=job.id,
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)
        unit = HistoryUnit(
            history_run_id=run.id,
            status=HistoryUnitStatus.SUBMITTED,
            logfile="/log/a",
            zarr_urls=["/a"],
        )
        db.add(unit)
        await db.commit()
        await db.refresh(unit)
        db.add(
            HistoryImageCache(
                zarr_url="/a",
                workflowtask_id=wftask.id,
                dataset_id=dataset.id,
                latest_history_unit_id=unit.id,
            )
        )
        await db.commit()

    if object_to_delete == "project":
        res = await client.delete(f"/api/v2/project/{project.id}/")
        assert res.status_code == 204
        res = await db.execute(select(HistoryImageCache))
        assert len(res.scalars().all()) == 0
        res = await db.execute(select(HistoryRun))
        assert len(res.scalars().all()) == 0
        res = await db.execute(select(HistoryUnit))
        assert len(res.scalars().all()) == 0

    elif object_to_delete == "dataset":
        res = await client.delete(
            f"/api/v2/project/{project.id}/dataset/{dataset.id}/"
        )
        assert res.status_code == 204
        res = await db.execute(select(HistoryImageCache))
        assert len(res.scalars().all()) == 0
        res = await db.execute(select(HistoryRun))
        assert len(res.scalars().all()) == 0
        res = await db.execute(select(HistoryUnit))
        assert len(res.scalars().all()) == 0

    elif object_to_delete == "workflowtask":
        res = await client.delete(
            f"/api/v2/project/{project.id}/"
            f"workflow/{workflow.id}/wftask/{wftask.id}/"
        )
        assert res.status_code == 204
        res = await db.execute(select(HistoryImageCache))
        assert len(res.scalars().all()) == 0
        res = await db.execute(select(HistoryRun))
        assert len(res.scalars().all()) == 1
        res = await db.execute(select(HistoryUnit))
        assert len(res.scalars().all()) == 1

    elif object_to_delete == "workflow":
        res = await client.delete(
            f"/api/v2/project/{project.id}/workflow/{workflow.id}/"
        )
        assert res.status_code == 204
        res = await db.execute(select(HistoryImageCache))
        assert len(res.scalars().all()) == 0
        res = await db.execute(select(HistoryRun))
        assert len(res.scalars().all()) == 1
        res = await db.execute(select(HistoryUnit))
        assert len(res.scalars().all()) == 1

    else:
        raise ValueError(f"Invalid {object_to_delete=}")


async def test_get_history_run_list(
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
    local_tz = datetime.now().astimezone().tzinfo
    timestamp = datetime.now(tz=local_tz)

    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task1 = await task_factory(user_id=user.id, version="3.1.4")
        task2 = await task_factory(
            user_id=user.id, args_schema_parallel={"foo": "bar"}, version="1.2"
        )
        debug(task1, task2)
        wftask1 = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task1.id
        )
        wftask2 = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task2.id
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )

        hr1 = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask1.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.DONE,
            num_available_images=1000,
            timestamp_started=timestamp,
            job_id=job.id,
            task_id=task1.id,
        )
        hr2 = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask1.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.SUBMITTED,
            num_available_images=2000,
            timestamp_started=timestamp,
            job_id=job.id,
            task_id=task1.id,
        )
        hr3 = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask2.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.FAILED,
            num_available_images=2000,
            timestamp_started=timestamp,
            job_id=job.id,
            task_id=task2.id,
        )
        hr4 = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask2.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.FAILED,
            num_available_images=2000,
            timestamp_started=timestamp,
            job_id=job.id,
            task_id=None,
        )
        for hr in [hr1, hr2, hr3, hr4]:
            db.add(hr)
        await db.commit()
        for hr in [hr1, hr2, hr3, hr4]:
            await db.refresh(hr)

        def add_units(hr_id: int, quantity: int, status: HistoryUnitStatus):
            for _ in range(quantity):
                db.add(
                    HistoryUnit(
                        history_run_id=hr_id,
                        status=status,
                        logfile="/fake/log",
                    )
                )

        add_units(hr1.id, 10, HistoryUnitStatus.DONE)
        add_units(hr1.id, 11, HistoryUnitStatus.SUBMITTED)
        add_units(hr1.id, 12, HistoryUnitStatus.FAILED)

        add_units(hr2.id, 20, HistoryUnitStatus.DONE)
        add_units(hr2.id, 21, HistoryUnitStatus.SUBMITTED)
        add_units(hr2.id, 22, HistoryUnitStatus.FAILED)

        await db.commit()

        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/"
            f"?workflowtask_id={wftask1.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json() == [
            {
                "id": hr1.id,
                "num_done_units": 10,
                "num_submitted_units": 11,
                "num_failed_units": 12,
                "timestamp_started": timestamp.isoformat(),
                "workflowtask_dump": {},
                "args_schema_non_parallel": None,
                "args_schema_parallel": None,
                "version": "3.1.4",
            },
            {
                "id": hr2.id,
                "num_done_units": 20,
                "num_submitted_units": 21,
                "num_failed_units": 22,
                "timestamp_started": timestamp.isoformat(),
                "workflowtask_dump": {},
                "args_schema_non_parallel": None,
                "args_schema_parallel": None,
                "version": "3.1.4",
            },
        ]

        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/"
            f"?workflowtask_id={wftask2.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json() == [
            {
                "id": hr3.id,
                "num_done_units": 0,
                "num_submitted_units": 0,
                "num_failed_units": 0,
                "timestamp_started": timestamp.isoformat(),
                "workflowtask_dump": {},
                "args_schema_non_parallel": None,
                "args_schema_parallel": {"foo": "bar"},
                "version": "1.2",
            },
            {
                "id": hr4.id,
                "num_done_units": 0,
                "num_submitted_units": 0,
                "num_failed_units": 0,
                "timestamp_started": timestamp.isoformat(),
                "workflowtask_dump": {},
                "args_schema_non_parallel": None,
                "args_schema_parallel": None,
                "version": None,
            },
        ]


async def test_get_history_run_units(
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
        wftask = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )
        hr = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            job_id=job.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.DONE,
            num_available_images=1000,
        )
        db.add(hr)
        await db.commit()
        await db.refresh(hr)

        for _ in range(6):
            db.add(
                HistoryUnit(
                    history_run_id=hr.id,
                    status=HistoryUnitStatus.DONE,
                    logfile="/fake/log",
                )
            )
        for _ in range(7):
            db.add(
                HistoryUnit(
                    history_run_id=hr.id,
                    status=HistoryUnitStatus.FAILED,
                    logfile="/fake/log",
                )
            )
        await db.commit()

        # 404
        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/1000/units/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 404

        # 422
        wftask2 = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/{hr.id}/units/"
            f"?workflowtask_id={wftask2.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 422
        dataset2 = await dataset_factory(project_id=project.id)
        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/{hr.id}/units/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset2.id}"
        )
        assert res.status_code == 422

        # Default pagination
        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/{hr.id}/units/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        res = res.json()
        assert res["current_page"] == 1
        assert res["page_size"] == 13
        assert res["total_count"] == 13
        assert len(res["items"]) == 13

        # With pagination parameters
        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/{hr.id}/units/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
            "&page=4&page_size=4"
        )
        assert res.status_code == 200
        res = res.json()
        assert res["current_page"] == 4
        assert res["page_size"] == 4
        assert res["total_count"] == 13
        assert len(res["items"]) == 1

        # With 'unit_status' query parameter
        res = await client.get(
            f"/api/v2/project/{project.id}/status/run/{hr.id}/units/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}"
            "&unit_status=done"
            "&page=1&page_size=5"
        )
        assert res.status_code == 200
        res = res.json()
        assert res["current_page"] == 1
        assert res["page_size"] == 5
        assert res["total_count"] == 6
        assert len(res["items"]) == 5


async def test_get_history_images(
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

        images_x_no_y = [
            SingleImage(
                zarr_url=f"/a{i}",
                types={"x": True},
                attributes={
                    "well": f"well-{i}",
                    "a_or_b": "a",
                },
            ).model_dump()
            for i in range(5)
        ]
        images_x_and_y = [
            SingleImage(
                zarr_url=f"/b{i}",
                types={"x": True, "is_b": True},
                attributes={
                    "well": f"well-{i}",
                    "a_or_b": "b",
                },
            ).model_dump()
            for i in range(5)
        ]

        dataset_images = images_x_no_y + images_x_and_y
        dataset = await dataset_factory(
            project_id=project.id, images=dataset_images
        )
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id, input_types={"y": True})

        wftask = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task.id,
            type_filters={"x": True},
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )

        hr = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.DONE,
            num_available_images=9999,
            job_id=job.id,
        )
        db.add(hr)
        await db.commit()

        hu = HistoryUnit(
            history_run_id=hr.id,
            logfile="/fake/log",
            status=HistoryUnitStatus.DONE,
            zarr_urls=["/a1", "/b1"],
        )
        db.add(hu)
        await db.commit()

        db.add(
            HistoryImageCache(
                zarr_url="/a1",
                dataset_id=dataset.id,
                workflowtask_id=wftask.id,
                latest_history_unit_id=hu.id,
            )
        )
        db.add(
            HistoryImageCache(
                zarr_url="/b1",
                dataset_id=dataset.id,
                workflowtask_id=wftask.id,
                latest_history_unit_id=hu.id,
            )
        )
        await db.commit()

        # CASE 1: No status filter, no type/attribute filters
        res = await client.post(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 200
        res = res.json()
        assert res["current_page"] == 1
        assert res["page_size"] == 10
        assert res["total_count"] == 10
        assert set(res["types"]) == {"x", "is_b"}
        assert set(res["attributes"]) == {"well", "a_or_b", IMAGE_STATUS_KEY}

        # CASE 2: status=unset filter, no type/attribute filters
        res = await client.post(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}",
            json=dict(
                attribute_filters={
                    IMAGE_STATUS_KEY: [HistoryUnitStatusWithUnset.UNSET]
                }
            ),
        )
        assert res.status_code == 200
        assert res.json()["total_count"] == 8
        for img in res.json()["items"]:
            assert (
                img["attributes"][IMAGE_STATUS_KEY]
                == HistoryUnitStatusWithUnset.UNSET
            )

        # CASE 3: status=done filter, no type/attribute filters
        res = await client.post(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}",
            json=dict(
                attribute_filters={
                    IMAGE_STATUS_KEY: [HistoryUnitStatusWithUnset.DONE]
                }
            ),
        )
        assert res.status_code == 200
        assert res.json()["total_count"] == 2
        assert res.json()["attributes"] == {
            IMAGE_STATUS_KEY: [
                "done",
                "submitted",
                "failed",
                "unset",
            ],
            "a_or_b": ["a", "b"],
            "well": [f"well-{i}" for i in range(5)],
        }
        for img in res.json()["items"]:
            assert (
                img["attributes"][IMAGE_STATUS_KEY]
                == HistoryUnitStatusWithUnset.DONE
            )

        # CASE 4: no status filter, some attribute filters
        res = await client.post(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}",
            json=dict(attribute_filters={"well": ["well-1"]}),
        )
        assert res.status_code == 200
        assert res.json()["total_count"] == 2
        assert res.json()["attributes"] == {
            IMAGE_STATUS_KEY: [
                "done",
                "submitted",
                "failed",
                "unset",
            ],
            "a_or_b": ["a", "b"],
            "well": [f"well-{i}" for i in range(5)],
        }
        for img in res.json()["items"]:
            assert img["attributes"]["well"] == "well-1"
            assert (
                img["attributes"][IMAGE_STATUS_KEY]
                == HistoryUnitStatusWithUnset.DONE
            )

        # CASE 5: no status filter, some type filters
        res = await client.post(
            f"/api/v2/project/{project.id}/status/images/"
            f"?workflowtask_id={wftask.id}&dataset_id={dataset.id}",
            json=dict(type_filters={"is_b": True}),
        )
        assert res.status_code == 200
        assert res.json()["attributes"] == {
            IMAGE_STATUS_KEY: [
                "done",
                "submitted",
                "failed",
                "unset",
            ],
            "a_or_b": ["a", "b"],
            "well": [f"well-{i}" for i in range(5)],
        }
        assert set(res.json()["types"]) == {"x", "is_b"}
        assert res.json()["total_count"] == 5


async def test_get_logs(
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    workflowtask_factory,
    job_factory,
    db,
    tmp_path,
    client,
    MockCurrentUser,
):
    ZARR_URL = "/zarr"
    LOGFILE = (tmp_path / "log").as_posix()
    LOGS = "something nice"
    with open(LOGFILE, "w") as f:
        f.write(LOGS)

    async with MockCurrentUser() as user:
        proj = await project_factory(user)
        ds = await dataset_factory(
            project_id=proj.id, images=[dict(zarr_url=ZARR_URL)]
        )
        wf = await workflow_factory(project_id=proj.id)
        task = await task_factory(user_id=user.id)
        wftask = await workflowtask_factory(workflow_id=wf.id, task_id=task.id)
        job = await job_factory(
            project_id=proj.id,
            dataset_id=ds.id,
            workflow_id=wf.id,
            working_dir="/foo",
            status="done",
        )

        run = HistoryRun(
            dataset_id=ds.id,
            workflowtask_id=wftask.id,
            workflowtask_dump={},
            task_group_dump={},
            status=HistoryUnitStatus.DONE,
            num_available_images=1,
            job_id=job.id,
        )
        db.add(run)
        await db.commit()
        await db.refresh(run)
        history_run_id = run.id

        unit = HistoryUnit(
            history_run_id=history_run_id,
            logfile=LOGFILE,
            status=HistoryUnitStatus.DONE,
            zarr_urls=[ZARR_URL],
        )
        db.add(unit)
        await db.commit()
        await db.refresh(unit)
        history_unit_id = unit.id

        db.add(
            HistoryImageCache(
                zarr_url=ZARR_URL,
                dataset_id=ds.id,
                workflowtask_id=wftask.id,
                latest_history_unit_id=history_unit_id,
            )
        )
        await db.commit()

        res = await client.get(
            f"/api/v2/project/{proj.id}/status/unit-log/"
            f"?workflowtask_id={wftask.id}&dataset_id={ds.id}"
            f"&{history_run_id=}&{history_unit_id=}"
        )
        assert res.status_code == 200
        assert res.json() == LOGS

        res = await client.post(
            f"/api/v2/project/{proj.id}/status/image-log/",
            json=dict(
                workflowtask_id=wftask.id,
                dataset_id=ds.id,
                zarr_url=ZARR_URL,
            ),
        )
        assert res.status_code == 200
        assert res.json() == LOGS

        # Assert 422 for unit-log
        res = await client.get(
            f"/api/v2/project/{proj.id}/status/unit-log/"
            f"?workflowtask_id={wftask.id}&dataset_id={ds.id}"
            f"&history_run_id=1000&{history_unit_id=}"
        )
        assert res.status_code == 422
        assert "Invalid query parameters: HistoryUnit" in res.json()["detail"]
        ds2 = await dataset_factory(project_id=proj.id)
        wftask2 = await workflowtask_factory(workflow_id=wf.id, task_id=task.id)
        res = await client.get(
            f"/api/v2/project/{proj.id}/status/unit-log/"
            f"?workflowtask_id={wftask2.id}&dataset_id={ds.id}"
            f"&history_run_id={history_run_id}&{history_unit_id=}"
        )
        assert res.status_code == 422
        assert "Invalid query parameters: HistoryRun" in res.json()["detail"]
        res = await client.get(
            f"/api/v2/project/{proj.id}/status/unit-log/"
            f"?workflowtask_id={wftask.id}&dataset_id={ds2.id}"
            f"&history_run_id={history_run_id}&{history_unit_id=}"
        )
        assert res.status_code == 422
        assert "Invalid query parameters: HistoryRun" in res.json()["detail"]


async def test_get_history_run_dataset(
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    workflowtask_factory,
    job_factory,
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)

        dataset = await dataset_factory(project_id=project.id)
        wf = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id)
        await workflowtask_factory(workflow_id=wf.id, task_id=task.id)
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=wf.id,
            working_dir="/foo",
            status="done",
        )

        N = 5
        for _ in range(5):
            db.add(
                HistoryRun(
                    dataset_id=dataset.id,
                    workflowtask_dump={},
                    task_group_dump={},
                    status=HistoryUnitStatus.DONE,
                    num_available_images=0,
                    job_id=job.id,
                )
            )
        await db.commit()

        res = await client.get(
            f"/api/v2/project/{project.id}/dataset/{dataset.id}/history/"
        )
        assert res.status_code == 200
        history_run_list = res.json()
        # Assert HistoryRuns are returned in reverse order
        assert [hr["id"] for hr in history_run_list] == list(range(1, N + 1))
