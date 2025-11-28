from pathlib import Path

import pytest
from devtools import debug  # noqa: F401
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import HistoryUnitStatusWithUnset
from fractal_server.images.status_tools import IMAGE_STATUS_KEY
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.executors.local.runner import LocalRunner
from fractal_server.urls import normalize_url

from .aux_get_dataset_attrs import _get_dataset_attrs
from .execute_tasks_v2 import execute_tasks_mod


async def _find_last_history_unit(db: AsyncSession) -> HistoryUnit:
    res = await db.execute(
        select(HistoryUnit)
        .join(HistoryRun, HistoryRun.id == HistoryUnit.history_run_id)
        .order_by(HistoryRun.timestamp_started.desc())
    )
    last_history_unit = res.scalars().first()
    return last_history_unit


async def _find_last_history_run(db: AsyncSession) -> HistoryUnit:
    res = await db.execute(
        select(HistoryRun).order_by(HistoryRun.timestamp_started.desc())
    )
    last_history_run = res.scalars().first()
    return last_history_run


async def add_history_image_cache(
    db,
    dataset_id: int,
    wftask_id: int,
    job_id: int,
    zarr_urls: list[str],
    status: str = "submitted",
):
    hr = HistoryRun(
        dataset_id=dataset_id,
        workflowtask_id=wftask_id,
        job_id=job_id,
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
def local_runner(
    tmp_path,
    local_resource_profile_objects,
):
    root_dir_local = tmp_path / "job"
    resource, profile = local_resource_profile_objects[:]
    with LocalRunner(
        root_dir_local=root_dir_local,
        resource=resource,
        profile=profile,
    ) as r:
        yield r


async def test_dummy_insert_single_image(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
    local_resource_profile_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id
    resource, profile = local_resource_profile_db

    async with MockCurrentUser(user_kwargs={"profile_id": profile.id}) as user:
        execute_tasks_v2_args = dict(
            runner=local_runner,
            user_id=user.id,
        )
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    # Case 0: Run successfully on an empty dataset
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        job_id=job.id,
        **execute_tasks_v2_args,
    )

    # Case 1: Run successfully even if the image already exists
    db.expunge_all()
    dataset = await db.get(DatasetV2, dataset.id)
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job1",
        job_id=job.id,
        **execute_tasks_v2_args,
    )

    # Case 2: Run successfully even if the image already exists but the new
    # image has an origin - see issue #2497.
    zarr_url_3D = Path(zarr_dir, "plate.zarr/B03").as_posix()
    zarr_url_2D = Path(zarr_dir, "plate_mip.zarr/B03").as_posix()
    IMAGES = [
        dict(
            zarr_url=zarr_url_3D,
            attributes={"well": "B03"},
            types={
                "is_3D": True,
                "illumination_corrected": True,
            },
        ),
        dict(
            zarr_url=zarr_url_2D,
            attributes={"well": "B03"},
            types={
                "is_3D": False,
                "illumination_corrected": True,
                "this_should_not_be_propagated": True,
            },
        ),
    ]
    dataset_case_2 = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=IMAGES,
    )
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        args_non_parallel={
            "full_new_image": dict(
                zarr_url=zarr_url_2D,
                origin=zarr_url_3D,
                types={
                    "is_3D": False,  # Make it look like a projection task
                    "some_additional_type": False,
                },
            )
        },
    )
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset_case_2,
        workflow_dir_local=tmp_path / "job2",
        job_id=job.id,
        **execute_tasks_v2_args,
    )
    db.expunge_all()
    dataset_case_2 = await db.get(DatasetV2, dataset_case_2.id)
    debug(dataset_case_2.images)
    assert dataset_case_2.images[0] == {
        "zarr_url": zarr_url_3D,
        "origin": None,
        "attributes": {"well": "B03"},
        "types": {
            "is_3D": True,
            "illumination_corrected": True,
        },
    }
    assert dataset_case_2.images[1] == {
        "zarr_url": zarr_url_2D,
        "origin": zarr_url_3D,
        "attributes": {"well": "B03"},
        "types": {
            "is_3D": False,
            "illumination_corrected": True,
            "some_additional_type": False,
        },
    }

    # Case 3: Fail because the new zarr_url is not relative to zarr_dir, or
    # because it is identical to zarr_dir
    EXPECTED_NON_PARENT_MSG = (
        "Cannot create image if zarr_url is not a subfolder of zarr_dir"
    )
    execute_tasks_v2_args = dict(
        runner=local_runner,
        user_id=user.id,
    )
    for _args_non_parallel in [{"fail": True}, {"fail_2": True}]:
        debug(_args_non_parallel)

        wftask = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task_id,
            args_non_parallel=_args_non_parallel,
        )
        db.expunge_all()
        dataset = await db.get(DatasetV2, dataset.id)
        with pytest.raises(JobExecutionError) as e:
            execute_tasks_mod(
                wf_task_list=[wftask],
                dataset=dataset,
                workflow_dir_local=tmp_path / "job3",
                job_id=job.id,
                **execute_tasks_v2_args,
            )
        error_msg = str(e.value)
        debug(error_msg)
        assert EXPECTED_NON_PARENT_MSG in error_msg
        assert zarr_dir in error_msg


async def test_dummy_remove_images(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    """
    NOTE: this test is relevant for
    https://github.com/fractal-analytics-platform/fractal-server/issues/2427.
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_remove_images"].id

    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )

    # Run successfully on a dataset which includes the images to be
    # removed
    project = await project_factory(user)
    N = 3
    dataset = await dataset_factory(
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

    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    await add_history_image_cache(
        db=db,
        dataset_id=dataset.id,
        wftask_id=wftask.id,
        job_id=job.id,
        zarr_urls=[img["zarr_url"] for img in dataset.images] + ["/foo"],
    )

    await db.refresh(dataset)
    assert len(dataset.images) == N
    res = await db.execute(select(func.count(HistoryImageCache.zarr_url)))
    assert res.scalar() == N + 1

    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        job_id=job.id,
        runner=local_runner,
    )

    await db.refresh(dataset)
    assert len(dataset.images) == 0
    res = await db.execute(select(func.count(HistoryImageCache.zarr_url)))
    assert res.scalar() == 1

    # Fail when removing images that do not exist
    dataset_pre_fail = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=[dict(zarr_url=Path(zarr_dir, "another-image").as_posix())],
    )
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(
            more_zarr_urls=[Path(zarr_dir, "missing-image").as_posix()]
        ),
    )
    with pytest.raises(JobExecutionError) as e:
        execute_tasks_mod(
            wf_task_list=[wftask],
            dataset=dataset_pre_fail,
            workflow_dir_local=tmp_path / "job1",
            user_id=user_id,
            job_id=job.id,
            runner=local_runner,
        )
    error_msg = str(e.value)
    assert "Cannot remove missing image" in error_msg


async def test_dummy_unset_attribute(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_unset_attribute"].id

    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    workflow = await workflow_factory(project_id=project.id)

    # Unset an existing attribute (starting from dataset_pre)
    dataset1 = await dataset_factory(
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
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(attribute="key2"),
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset1.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset1,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        job_id=job.id,
        runner=local_runner,
    )
    db.expunge_all()
    dataset_attrs = await _get_dataset_attrs(db, dataset1.id)
    debug(dataset_attrs["images"])
    assert "key2" not in dataset_attrs["images"][0]["attributes"].keys()

    # Unset a missing attribute (starting from dataset_pre)
    dataset2 = await dataset_factory(
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
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(attribute="missing-attribute"),
    )
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset2,
        workflow_dir_local=tmp_path / "job1",
        user_id=user_id,
        job_id=job.id,
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
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(attributes={"attribute-name": None}),
    )
    # Run successfully on an empty dataset
    dataset = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        job_id=job.id,
        runner=local_runner,
    )
    # Assert that attribute was not set
    await db.refresh(dataset)
    assert "attribute-name" not in dataset.images[0]["attributes"].keys()


async def test_dummy_insert_single_image_normalization(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel={"trailing_slash": True},
    )
    # Run successfully on an empty dataset
    dataset = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        job_id=job.id,
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
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
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
        project = await project_factory(user)
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
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
    dataset = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=images,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )
    execute_tasks_mod(
        wf_task_list=[wftask],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        job_id=job.id,
        runner=local_runner,
    )

    # Assert that images were included by default
    await db.refresh(dataset)
    debug(dataset)
    assert dataset.images[0]["types"] == dict(my_type=True)


async def test_compound_task_with_compute_failure(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    # Preliminary setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["generic_task_compound"].id
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
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
    dataset = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=images,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )
    with pytest.raises(JobExecutionError) as exc_info:
        execute_tasks_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job0",
            user_id=user_id,
            job_id=job.id,
            runner=local_runner,
        )
    debug(exc_info.value.assemble_error())
    assert "raise_error=True" in exc_info.value.assemble_error()


async def test_dummy_invalid_output_non_parallel(
    db,
    MockCurrentUser,
    monkeypatch,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # case non-parallel
    task_id = fractal_tasks_mock_db["dummy_insert_single_image"].id
    async with MockCurrentUser() as user:
        execute_tasks_v2_args = dict(
            runner=local_runner,
            user_id=user.id,
        )
        project = await project_factory(user)
    IMAGES = [
        dict(
            zarr_url=Path(zarr_dir, "my-image").as_posix(),
            types={},
            attributes={},
        )
    ]
    dataset = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, images=IMAGES
    )
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )

    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    import fractal_server.runner.v2.runner_functions
    from fractal_server.runner.exceptions import TaskOutputValidationError

    def patched_cast(*args, **kwargs):
        raise TaskOutputValidationError()

    monkeypatch.setattr(
        fractal_server.runner.v2.runner_functions,
        "_cast_and_validate_TaskOutput",
        patched_cast,
    )
    with pytest.raises(JobExecutionError):
        execute_tasks_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job0",
            job_id=job.id,
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


async def test_dummy_invalid_output_parallel(
    db,
    MockCurrentUser,
    monkeypatch,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["generic_task_parallel"].id
    async with MockCurrentUser() as user:
        execute_tasks_v2_args = dict(
            runner=local_runner,
            user_id=user.id,
        )
        project = await project_factory(user)
    IMAGES = [
        dict(
            zarr_url=Path(zarr_dir, "my-image").as_posix(),
            types={},
            attributes={},
        )
    ]

    dataset = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, images=IMAGES
    )
    workflow = await workflow_factory(project_id=project.id)
    wftask = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    import fractal_server.runner.v2.runner_functions
    from fractal_server.runner.exceptions import TaskOutputValidationError
    from fractal_server.runner.v2.runner_functions import SubmissionOutcome

    def patched_task_output(*args, **kwargs):
        debug("XXX")
        return SubmissionOutcome(
            exception=TaskOutputValidationError(), invalid_output=True
        )

    monkeypatch.setattr(
        fractal_server.runner.v2.runner_functions,
        "_process_task_output",
        patched_task_output,
    )
    with pytest.raises(JobExecutionError):
        execute_tasks_mod(
            wf_task_list=[wftask],
            dataset=dataset,
            workflow_dir_local=tmp_path / "job0",
            job_id=job.id,
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


async def test_status_based_submission(
    db,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    workflowtask_factory,
    job_factory,
    tmp_path: Path,
    local_runner: LocalRunner,
    fractal_tasks_mock_db,
):
    """
    Test processing of images based on status.
    """

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    task_id = fractal_tasks_mock_db["generic_task"].id

    IMAGES = [
        dict(
            zarr_url=Path(zarr_dir, f"plate.zarr/B0{ind}").as_posix(),
            attributes={"plate": "plate.zarr", "well": f"B0{ind}"},
            types={"is_3D": True},
        )
        for ind in range(1, 5)
    ]

    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    workflow = await workflow_factory(project_id=project.id)
    wftask_failing = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
        args_non_parallel=dict(raise_error=True),
    )
    wftask_ok = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=task_id,
        order=0,
    )

    # Case 1: Run and fail for B00 and B01 (by requiring the UNSET ones)
    dataset = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=IMAGES,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
    )
    with pytest.raises(JobExecutionError):
        execute_tasks_mod(
            wf_task_list=[wftask_failing],
            dataset=dataset,
            workflow_dir_local=tmp_path / str(job.id),
            job_id=job.id,
            runner=local_runner,
            user_id=user_id,
            job_attribute_filters={
                "well": ["B01", "B02"],
                IMAGE_STATUS_KEY: [HistoryUnitStatusWithUnset.UNSET],
            },
        )

    # Check that `HistoryImageCache`/`HistoryUnit` data were stored correctly
    for ind in [1, 2]:
        zarr_url = Path(zarr_dir, f"plate.zarr/B0{ind}").as_posix()
        res = await db.execute(
            select(HistoryImageCache).where(
                HistoryImageCache.zarr_url == zarr_url
            )
        )
        history_image_cache = res.scalar_one()
        debug(history_image_cache)
        history_unit = await db.get(
            HistoryUnit,
            history_image_cache.latest_history_unit_id,
        )
        debug(history_unit)
        assert history_unit.status == HistoryUnitStatusWithUnset.FAILED

    # Case 1: Run and fail for no images (by requiring the DONE ones)
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
    )
    with pytest.raises(JobExecutionError, match="empty image list"):
        execute_tasks_mod(
            wf_task_list=[wftask_ok],
            dataset=dataset,
            workflow_dir_local=tmp_path / str(job.id),
            job_id=job.id,
            runner=local_runner,
            user_id=user_id,
            job_attribute_filters={
                IMAGE_STATUS_KEY: [HistoryUnitStatusWithUnset.DONE],
            },
        )

    # Validate latest `HistoryRun` object
    last_history_run = await _find_last_history_run(db)
    assert last_history_run.status == HistoryUnitStatus.FAILED

    # Case 1: Run and succeed for no images (by requiring the UNSET ones)
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
    )
    execute_tasks_mod(
        wf_task_list=[wftask_ok],
        dataset=dataset,
        workflow_dir_local=tmp_path / str(job.id),
        job_id=job.id,
        runner=local_runner,
        user_id=user_id,
        job_attribute_filters={
            IMAGE_STATUS_KEY: [HistoryUnitStatusWithUnset.UNSET],
        },
    )

    res = await db.execute(
        select(HistoryImageCache).order_by(HistoryImageCache.zarr_url)
    )
    debug(res.scalars().all())
    res = await db.execute(select(HistoryUnit).order_by(HistoryUnit.id))
    debug(res.scalars().all())

    # Case 2: Run successfully on all images
    dataset = await dataset_factory(
        project_id=project.id,
        zarr_dir=zarr_dir,
        images=IMAGES,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
    )
    execute_tasks_mod(
        wf_task_list=[wftask_ok],
        dataset=dataset,
        workflow_dir_local=tmp_path / str(job.id),
        job_id=job.id,
        runner=local_runner,
        user_id=user_id,
    )
