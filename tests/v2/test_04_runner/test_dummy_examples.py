from pathlib import Path

import pytest
from devtools import debug  # noqa: F401
from sqlmodel import func
from sqlmodel import select

from .aux_get_dataset_attrs import _get_dataset_attrs
from .execute_tasks_v2 import execute_tasks_v2_mod
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.executors.local.runner import LocalRunner
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.urls import normalize_url


async def add_history_image_cache(
    db,
    dataset_id: int,
    wftask_id: int,
    zarr_urls: list[str],
    status: str = "submitted",
):
    hr = HistoryRun(
        dataset_id=dataset_id,
        workflowtask_id=wftask_id,
        workflowtask_dump={},
        task_group_dump={},
        status=status,
        num_available_images=len(zarr_urls),
    )
    db.add(hr)
    await db.commit()
    await db.refresh(hr)

    hu = HistoryUnit(
        status=status,
        zarr_url=zarr_urls,
        history_run_id=hr.id,
        logfile="/fake/logs",
    )
    db.add(hu)
    await db.commit()
    await db.refresh(hu)

    for zarr_url in zarr_urls:
        db.add(
            HistoryImageCache(
                dataset_id=dataset_id,
                workflowtask_id=wftask_id,
                zarr_url=zarr_url,
                latest_history_unit_id=hu.id,
            )
        )
    await db.commit()


@pytest.fixture()
def local_runner(tmp_path):
    root_dir_local = tmp_path / "job"
    with LocalRunner(root_dir_local=root_dir_local) as r:
        yield r


async def test_dummy_insert_single_image(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id

    async with MockCurrentUser() as user:
        execute_tasks_v2_args = dict(
            runner=local_runner,
            user_id=user.id,
        )
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(
        project_id=project.id, zarr_dir=zarr_dir
    )
    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )

    # Run successfully on an empty dataset
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        **execute_tasks_v2_args,
    )

    # Run successfully even if the image already exists
    db.expunge_all()
    dataset = await db.get(DatasetV2, dataset.id)
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job1",
        **execute_tasks_v2_args,
    )

    # Fail because new image is not relative to zarr_dir
    execute_tasks_v2_args = dict(
        runner=local_runner,
        user_id=user.id,
    )
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={"fail": True},
    )
    db.expunge_all()
    dataset = await db.get(DatasetV2, dataset.id)
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job3",
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert "is not a parent directory" in error_msg
    assert zarr_dir in error_msg

    # Fail because new image's zarr_url is equal to zarr_dir
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={"fail_2": True},
    )
    db.expunge_all()
    dataset = await db.get(DatasetV2, dataset.id)
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job4",
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert "Cannot create image if zarr_url is equal to zarr_dir" in error_msg

    # Fail because new image is not relative to zarr_dir
    IMAGES = [dict(zarr_url=Path(zarr_dir, "my-image").as_posix())]
    dataset_with_images = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=IMAGES,
    )
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={
            "full_new_image": dict(
                zarr_url=IMAGES[0]["zarr_url"],
                origin="/somewhere",
            )
        },
    )
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset_with_images,
            workflow_dir_local=tmp_path / "job2",
            **execute_tasks_v2_args,
        )
    error_msg = str(e.value)
    assert (
        "Cannot edit an image with zarr_url different from origin."
        in error_msg
    )


async def test_dummy_remove_images(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_remove_images"].id

    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory_v2(user)

    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )

    # Run successfully on a dataset which includes the images to be
    # removed
    project = await project_factory_v2(user)
    N = 3
    dataset = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=[
            dict(zarr_url=Path(zarr_dir, str(index)).as_posix())
            for index in range(N)
        ],
    )

    assert len(dataset.images) == N
    res = await db.execute(select(func.count(HistoryImageCache.zarr_url)))
    assert res.scalar() == 0

    await add_history_image_cache(
        db=db,
        dataset_id=dataset.id,
        wftask_id=wftask.id,
        zarr_urls=[img["zarr_url"] for img in dataset.images] + ["/foo"],
    )

    await db.refresh(dataset)
    assert len(dataset.images) == N
    res = await db.execute(select(func.count(HistoryImageCache.zarr_url)))
    assert res.scalar() == N + 1

    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
    )

    await db.refresh(dataset)
    assert len(dataset.images) == 0
    res = await db.execute(select(func.count(HistoryImageCache.zarr_url)))
    assert res.scalar() == 1

    # Fail when removing images that do not exist
    dataset_pre_fail = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
    )
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(
            more_zarr_urls=[Path(zarr_dir, "missing-image").as_posix()]
        ),
    )
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset_pre_fail,
            workflow_dir_local=tmp_path / "job1",
            user_id=user_id,
            runner=local_runner,
        )
    error_msg = str(e.value)
    assert "Cannot remove missing image" in error_msg


async def test_dummy_unset_attribute(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_unset_attribute"].id

    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory_v2(user)

    workflow = await workflow_factory_v2(project_id=project.id)

    # Unset an existing attribute (starting from dataset_pre)
    dataset1 = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=[
            dict(
                zarr_url=Path(zarr_dir, "my-image").as_posix(),
                attributes={"key1": "value1", "key2": "value2"},
                types={},
            )
        ],
    )
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(attribute="key2"),
    )
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset1,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
    )
    db.expunge_all()
    dataset_attrs = await _get_dataset_attrs(db, dataset1.id)
    debug(dataset_attrs["images"])
    assert "key2" not in dataset_attrs["images"][0]["attributes"].keys()

    # Unset a missing attribute (starting from dataset_pre)
    dataset2 = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=[
            dict(
                zarr_url=Path(zarr_dir, "my-image").as_posix(),
                attributes={"key1": "value1", "key2": "value2"},
                types={},
            )
        ],
    )
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(attribute="missing-attribute"),
    )
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset2,
        workflow_dir_local=tmp_path / "job1",
        user_id=user_id,
        runner=local_runner,
    )
    db.expunge_all()
    dataset_attrs = await _get_dataset_attrs(db, dataset2.id)
    assert dataset_attrs["images"][0]["attributes"] == {
        "key1": "value1",
        "key2": "value2",
    }


async def test_dummy_insert_single_image_with_attribute_none(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory_v2(user)
    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(attributes={"attribute-name": None}),
    )

    # Run successfully on an empty dataset
    dataset = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
    )
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
    )
    # Assert that attribute was not set
    await db.refresh(dataset)
    assert "attribute-name" not in dataset.images[0]["attributes"].keys()


async def test_dummy_insert_single_image_normalization(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory_v2(user)
    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={"trailing_slash": True},
    )
    # Run successfully on an empty dataset
    dataset = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
    )
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
    )
    # Assert that URLs are normalized
    await db.refresh(dataset)
    debug(dataset.images)
    for image in dataset.images:
        assert normalize_url(image["zarr_url"]) == image["zarr_url"]


async def test_default_inclusion_of_images(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    """
    Ref
    https://github.com/fractal-analytics-platform/fractal-server/issues/1374
    """
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["generic_task_parallel"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory_v2(user)
    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={"trailing_slash": True},
    )

    # Run successfully
    images = [
        dict(
            zarr_url=Path(zarr_dir, "my_image").as_posix(),
            attributes={},
            types={},
        )
    ]
    dataset = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=images,
    )
    execute_tasks_v2_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
    )

    # Assert that images were included by default
    await db.refresh(dataset)
    debug(dataset)
    assert dataset.images[0]["types"] == dict(my_type=True)


async def test_compound_task_with_compute_failure(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["generic_task_compound"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory_v2(user)
    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={"argument": 3},
        args_parallel={"raise_error": True},  # make it fail
    )

    images = [
        dict(
            zarr_url=Path(zarr_dir, "my_image").as_posix(),
            attributes={},
            types={},
        )
    ]

    # Run and fail
    dataset = await dataset_factory_v2(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=images,
    )
    with pytest.raises(JobExecutionError) as exc_info:
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job0",
            user_id=user_id,
            runner=local_runner,
        )
    debug(exc_info.value.assemble_error())
    assert "raise_error=True" in exc_info.value.assemble_error()


async def test_dummy_invalid_output(
    db,
    MockCurrentUser,
    monkeypatch,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id

    async with MockCurrentUser() as user:
        execute_tasks_v2_args = dict(
            runner=local_runner,
            user_id=user.id,
        )
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(
        project_id=project.id, zarr_dir=zarr_dir
    )
    workflow = await workflow_factory_v2(project_id=project.id)
    wftask = await workflowtask_factory_v2(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )

    import fractal_server.app.runner.v2.runner_functions
    from fractal_server.app.runner.exceptions import TaskOutputValidationError

    def patched_cast(*args, **kwargs):
        raise TaskOutputValidationError()

    monkeypatch.setattr(
        fractal_server.app.runner.v2.runner_functions,
        "_cast_and_validate_TaskOutput",
        patched_cast,
    )
    # Run successfully on an empty dataset
    with pytest.raises(JobExecutionError):
        execute_tasks_v2_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job0",
            **execute_tasks_v2_args,
        )
    res = await db.execute(
        select(HistoryRun).where(HistoryRun.dataset_id == dataset.id)
    )
    hi = res.scalar_one_or_none()
    res = await db.execute(
        select(HistoryUnit).where(HistoryUnit.history_run_id == hi.id)
    )
    hu = res.scalar_one_or_none()
    assert hu.status == HistoryUnitStatus.FAILED
