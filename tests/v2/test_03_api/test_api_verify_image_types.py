from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.images import SingleImage
from fractal_server.images.status_tools import IMAGE_STATUS_KEY
from tests.v2.test_03_api.test_api_workflow_task import PREFIX


async def test_verify_image_types(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    client,
):
    ZARR_DIR = "/zarr_dir"

    images = []
    index = 0
    well = "B03"
    for is_3D in [True, False, "unset"]:
        types = {"is_3D": is_3D} if is_3D != "unset" else {}
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types=types,
            ).model_dump()
        )
        index += 1
    for illum_corr in [True, "unset"]:
        types = {"illum_corr": illum_corr} if illum_corr != "unset" else {}
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types=types,
            ).model_dump()
        )
        index += 1
    for registered in [False, "unset"]:
        types = {"registered": registered} if registered != "unset" else {}
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types=types,
            ).model_dump()
        )
        index += 1
    well = "B04"
    for bad_type_3 in [True, False]:
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types={"bad_type_3": bad_type_3},
            ).model_dump()
        )
        index += 1

    async with MockCurrentUser() as user:
        project = await project_factory(user)

    dataset = await dataset_factory(
        project_id=project.id, zarr_dir=ZARR_DIR, images=images
    )

    FAKE_WFTASK_ID = 123
    url = (
        f"api/v2/project/{project.id}/dataset/{dataset.id}/"
        f"images/verify-unique-types/?workflowtask_id={FAKE_WFTASK_ID}"
    )

    # No filters
    res = await client.post(url)
    assert res.status_code == 200
    assert res.json() == ["bad_type_3", "illum_corr", "is_3D"]

    # Attribute filter
    res = await client.post(
        url,
        json={"attribute_filters": {"well": ["B03"]}},
    )
    assert res.status_code == 200
    assert res.json() == ["illum_corr", "is_3D"]

    # Attribute&type filters
    res = await client.post(
        url,
        json={
            "attribute_filters": {"well": ["B03"]},
            "type_filters": {"is_3D": False},
        },
    )
    assert res.status_code == 200
    assert res.json() == ["illum_corr"]

    # Attribute&type filters
    res = await client.post(
        url,
        json={
            "attribute_filters": {"well": ["B03"]},
            "type_filters": {"is_3D": True},
        },
    )
    assert res.status_code == 200
    assert res.json() == []


async def test_check_non_processed_images(
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    workflowtask_factory,
    job_factory,
    client,
    MockCurrentUser,
    tmp_path,
    db,
):
    """
    Test both the non-processed and verify-unique-types, with data which
    have non-trivial history.
    """

    async with MockCurrentUser() as user:
        task1 = await task_factory(
            user_id=user.id,
            name="a",
        )
        task2 = await task_factory(
            user_id=user.id,
            output_types={"x": True},
            name="b",
        )
        task3 = await task_factory(
            name="c",
            user_id=user.id,
            type="converter_non_parallel",
        )

        project = await project_factory(user)

        workflow = await workflow_factory(project_id=project.id)
        wft1 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task1.id,
        )
        wft2 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task2.id,
        )
        wft3 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task1.id,
        )
        await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task3.id,  # converter task
        )
        wft5 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task1.id,
        )

        n = 10
        dataset = await dataset_factory(
            project_id=project.id,
            zarr_dir="/zarr_dir",
            images=[
                SingleImage(
                    zarr_url=f"/zarr_dir/{i}",
                    types={"my_type": bool(i % 2)},
                ).model_dump()
                for i in range(n)
            ]
            + [
                SingleImage(
                    zarr_url="/another/image.zarr",
                ).model_dump()
            ],
        )
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="done",
        )

        hr = HistoryRun(
            dataset_id=dataset.id,
            workflowtask_id=wft2.id,
            job_id=job.id,
            workflowtask_dump={},
            task_group_dump={},
            status="done",
            num_available_images=n,
        )
        db.add(hr)
        await db.commit()
        await db.refresh(hr)

        hu1 = HistoryUnit(
            history_run_id=hr.id,
            logfile="file1.log",
            status="done",
            zarr_urls=["/zarr_dir/0"],
        )
        hu2 = HistoryUnit(
            history_run_id=hr.id,
            logfile="file2.log",
            status="failed",
            zarr_urls=[f"/zarr_dir/{i}" for i in range(1, n)],
        )
        db.add_all([hu1, hu2])
        await db.commit()
        await db.refresh(hu1)
        await db.refresh(hu2)

        db.add(
            HistoryImageCache(
                zarr_url="/zarr_dir/0",
                dataset_id=dataset.id,
                workflowtask_id=wft1.id,
                latest_history_unit_id=hu1.id,
            )
        )
        for i in range(1, n):
            db.add(
                HistoryImageCache(
                    zarr_url=f"/zarr_dir/{i}",
                    dataset_id=dataset.id,
                    workflowtask_id=wft1.id,
                    latest_history_unit_id=hu2.id,
                )
            )
        await db.commit()

        # case 1: first task in the workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            "images/non-processed/"
            f"?workflow_id={workflow.id}&workflowtask_id={wft1.id}",
            json={},
        )
        assert res.status_code == 200
        assert res.json() == []

        # case 2: previous task sets output_types
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            "images/non-processed/"
            f"?workflow_id={workflow.id}&workflowtask_id={wft3.id}",
            json={},
        )
        assert res.status_code == 200
        assert res.json() == []

        # case 3: previous task is converter
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            "images/non-processed/"
            f"?workflow_id={workflow.id}&workflowtask_id={wft5.id}",
            json={},
        )
        assert res.status_code == 200
        assert res.json() == []

        # case 4: actual check
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            "images/non-processed/"
            f"?workflow_id={workflow.id}&workflowtask_id={wft2.id}",
            json={},
        )
        assert res.status_code == 200
        assert set(res.json()) == set(
            ["/another/image.zarr"] + [f"/zarr_dir/{i}" for i in range(1, n)]
        )

        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            "images/non-processed/"
            f"?workflow_id={workflow.id}&workflowtask_id={wft2.id}",
            json={"type_filters": {"my_type": True}},
        )
        assert res.status_code == 200
        assert set(res.json()) == {
            f"/zarr_dir/{i}" for i in range(1, n) if i % 2
        }

    # Test verify-unique-types endpoint
    url = (
        f"api/v2/project/{project.id}/dataset/{dataset.id}/"
        f"images/verify-unique-types/?workflowtask_id={wft1.id}"
    )
    # Enter the branch where images are status-enriched
    res = await client.post(
        url,
        json=dict(
            attribute_filters={
                IMAGE_STATUS_KEY: [
                    "done",
                    "failed",
                    "submitted",
                    "unset",
                ]
            }
        ),
    )
    assert res.status_code == 200
    assert res.json() == ["my_type"]
