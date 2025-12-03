import os
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.images import SingleImage
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.runner.executors.local.runner import LocalRunner

from .aux_get_dataset_attrs import _get_dataset_attrs
from .execute_tasks import execute_tasks_mod


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


def _assert_image_data_exist(image_list: list[dict]):
    for image in image_list:
        assert (Path(image["zarr_url"]) / "data").exists()


def image_data_exist_on_disk(image_list: list[SingleImage]):
    """
    Given an image list, check whether mock data were written to disk.
    """
    prefix = "[image_data_exist_on_disk]"
    all_images_have_data = True
    for image in image_list:
        if (Path(image["zarr_url"]) / "data").exists():
            print(f"{prefix} {image['zarr_url']} contains data")
        else:
            print(f"{prefix} {image['zarr_url']} does *not* contain data")
            all_images_have_data = False
    return all_images_have_data


async def test_fractal_demos_01(
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
    """
    Mock of fractal-demos/examples/01.
    """
    resource, _ = local_resource_profile_db

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)

    wftask0 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["create_ome_zarr_compound"].id,
        order=0,
        args_non_parallel=dict(image_dir="/tmp/input_images"),
        args_parallel={},
    )
    wftask1 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["illumination_correction"].id,
        args_parallel=dict(overwrite_input=True),
        order=1,
    )
    wftask2 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["MIP_compound"].id,
        args_non_parallel=dict(suffix="mip"),
        args_parallel={},
        order=2,
    )
    wftask3 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["cellpose_segmentation"].id,
        args_parallel={},
        order=3,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )
    execute_tasks_mod(
        wf_task_list=[wftask0],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        runner=local_runner,
        user_id=user_id,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)
    _assert_image_data_exist(dataset_attrs["images"])
    assert len(dataset_attrs["images"]) == 2

    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask1],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job1",
        runner=local_runner,
        user_id=user_id,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)
    assert {img["zarr_url"] for img in dataset_attrs["images"]} == {
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    }

    img = find_image_by_zarr_url(
        zarr_url=f"{zarr_dir}/my_plate.zarr/A/01/0",
        images=dataset_attrs["images"],
    )["image"]
    assert img == {
        "zarr_url": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
        },
        "types": {
            "illumination_correction": True,
            "3D": True,
        },
        "origin": None,
    }

    _assert_image_data_exist(dataset_attrs["images"])
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask2],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job2",
        runner=local_runner,
        user_id=user_id,
        job_type_filters={
            "illumination_correction": True,
        },
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)
    debug(dataset_attrs)

    img = find_image_by_zarr_url(
        zarr_url=f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
        images=dataset_attrs["images"],
    )["image"]
    assert img == {
        "zarr_url": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
        "origin": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate_mip.zarr",
        },
        "types": {
            "3D": False,
            "illumination_correction": True,
        },
    }
    _assert_image_data_exist(dataset_attrs["images"])
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask3],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job3",
        runner=local_runner,
        user_id=user_id,
        job_type_filters={
            "illumination_correction": True,
            "3D": False,
        },
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)
    debug(dataset_attrs)


async def test_fractal_demos_01_no_overwrite(
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
    """
    Similar to fractal-demos/examples/01, but illumination
    correction task does not override its input images.
    """
    resource, _ = local_resource_profile_db

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)

    wftask0 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["create_ome_zarr_compound"].id,
        order=0,
        args_non_parallel=dict(image_dir="/tmp/input_images"),
        args_parallel={},
    )
    wftask1 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["illumination_correction"].id,
        args_parallel=dict(overwrite_input=False),
        order=1,
    )
    wftask2 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["MIP_compound"].id,
        args_non_parallel=dict(suffix="mip"),
        args_parallel={},
        order=2,
    )
    wftask3 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["cellpose_segmentation"].id,
        args_parallel={},
        order=3,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    execute_tasks_mod(
        wf_task_list=[wftask0],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        runner=local_runner,
        user_id=user_id,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)
    assert [img["zarr_url"] for img in dataset_attrs["images"]] == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    ]

    _assert_image_data_exist(dataset_attrs["images"])

    # Run illumination correction with overwrite_input=False
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask1],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job1",
        runner=local_runner,
        user_id=user_id,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    assert [img["zarr_url"] for img in dataset_attrs["images"]] == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
    ]
    assert dataset_attrs["images"][0] == {
        "zarr_url": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "origin": None,
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
        },
        "types": {
            "3D": True,
        },
    }
    assert dataset_attrs["images"][1] == {
        "zarr_url": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "origin": None,
        "attributes": {
            "well": "A02",
            "plate": "my_plate.zarr",
        },
        "types": {
            "3D": True,
        },
    }
    assert dataset_attrs["images"][2] == {
        "zarr_url": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        "origin": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
        },
        "types": {
            "illumination_correction": True,
            "3D": True,
        },
    }
    assert dataset_attrs["images"][3] == {
        "zarr_url": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        "origin": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "attributes": {
            "well": "A02",
            "plate": "my_plate.zarr",
        },
        "types": {
            "3D": True,
            "illumination_correction": True,
        },
    }
    _assert_image_data_exist(dataset_attrs["images"])
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask2],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job2",
        runner=local_runner,
        user_id=user_id,
        job_type_filters={
            "illumination_correction": True,
        },
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    assert [img["zarr_url"] for img in dataset_attrs["images"]] == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
    ]

    assert dataset_attrs["images"][4] == {
        "zarr_url": f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        "origin": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        "attributes": {
            "well": "A01",
            "plate": "my_plate_mip.zarr",
        },
        "types": {
            "3D": False,
            "illumination_correction": True,
        },
    }
    assert dataset_attrs["images"][5] == {
        "zarr_url": f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
        "origin": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        "attributes": {
            "well": "A02",
            "plate": "my_plate_mip.zarr",
        },
        "types": {
            "3D": False,
            "illumination_correction": True,
        },
    }
    _assert_image_data_exist(dataset_attrs["images"])
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask3],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job3",
        runner=local_runner,
        user_id=user_id,
        job_type_filters={
            "3D": False,
            "illumination_correction": True,
        },
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)


async def test_registration_no_overwrite(
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
    resource, _ = local_resource_profile_db
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)

    wftask0 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["create_ome_zarr_multiplex_compound"].id,
        order=0,
        args_non_parallel=dict(image_dir="/tmp/input_images"),
        args_parallel={},
    )
    wftask1 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["calculate_registration_compound"].id,
        args_non_parallel={"ref_acquisition": 0},
        order=1,
    )
    wftask2 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["find_registration_consensus"].id,
        order=2,
    )
    wftask3 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["apply_registration_to_image"].id,
        args_parallel={"overwrite_input": False},
        order=3,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    execute_tasks_mod(
        wf_task_list=[wftask0],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)

    # Run init registration
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask1],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job1",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # In all non-reference-cycle images, a certain table was updated
    for image in dataset_attrs["images"]:
        if image["attributes"]["acquisition"] == 0:
            assert not os.path.isfile(f"{image['zarr_url']}/registration_table")
        else:
            assert os.path.isfile(f"{image['zarr_url']}/registration_table")

    # Run find_registration_consensus
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask2],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job2",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # In all images, a certain (post-consensus) table was updated
    for image in dataset_attrs["images"]:
        assert os.path.isfile(f"{image['zarr_url']}/registration_table_final")

    # The image list still has the original six images only
    assert len(dataset_attrs["images"]) == 6

    # Run apply_registration_to_image
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask3],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job3",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # A new copy of each image was created
    assert len(dataset_attrs["images"]) == 12


async def test_registration_overwrite(
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
    resource, _ = local_resource_profile_db
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)

    wftask0 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["create_ome_zarr_multiplex_compound"].id,
        order=0,
        args_non_parallel=dict(image_dir="/tmp/input_images"),
        args_parallel={},
    )
    wftask1 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["calculate_registration_compound"].id,
        args_non_parallel={"ref_acquisition": 0},
        order=1,
    )
    wftask2 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["find_registration_consensus"].id,
        order=2,
    )
    wftask3 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["apply_registration_to_image"].id,
        args_parallel={"overwrite_input": True},
        order=3,
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    execute_tasks_mod(
        wf_task_list=[wftask0],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)

    # Run init registration
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask1],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job1",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # In all non-reference-cycle images, a certain table was updated
    for image in dataset_attrs["images"]:
        if image["attributes"]["acquisition"] == 0:
            assert not os.path.isfile(f"{image['zarr_url']}/registration_table")
        else:
            assert os.path.isfile(f"{image['zarr_url']}/registration_table")

    # Run find_registration_consensus
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask2],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job2",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # In all images, a certain (post-consensus) table was updated
    for image in dataset_attrs["images"]:
        assert os.path.isfile(f"{image['zarr_url']}/registration_table_final")

    # The image list still has the original six images only
    assert len(dataset_attrs["images"]) == 6

    # Run apply_registration_to_image
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask3],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job3",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # Images are still the same number, but they are marked as registered
    assert len(dataset_attrs["images"]) == 6
    for image in dataset_attrs["images"]:
        assert image["types"]["registration"] is True


async def test_channel_parallelization_with_overwrite(
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
    resource, _ = local_resource_profile_db
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)

    wftask0 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["create_ome_zarr_compound"].id,
        order=0,
        args_non_parallel=dict(image_dir="/tmp/input_images"),
    )
    wftask1 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["illumination_correction_compound"].id,
        order=1,
        args_non_parallel=dict(overwrite_input=True),
        args_parallel=dict(another_argument="something"),
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    # Run create_ome_zarr+yokogawa_to_zarr
    execute_tasks_mod(
        wf_task_list=[wftask0],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)

    # Run illumination_correction_compound
    db.expunge_all()
    dataset = await db.get(DatasetV2, dataset.id)
    execute_tasks_mod(
        wf_task_list=[wftask1],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job1",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)

    # Check that there are now 2 images
    assert len(dataset_attrs["images"]) == 2


async def test_channel_parallelization_no_overwrite(
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
    resource, _ = local_resource_profile_db
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)

    dataset = await dataset_factory(project_id=project.id, zarr_dir=zarr_dir)
    workflow = await workflow_factory(project_id=project.id)

    wftask0 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["create_ome_zarr_compound"].id,
        order=0,
        args_non_parallel=dict(image_dir="/tmp/input_images"),
    )
    wftask1 = await workflowtask_factory(
        workflow_id=workflow.id,
        task_id=fractal_tasks_mock_db["illumination_correction_compound"].id,
        order=1,
        args_non_parallel=dict(overwrite_input=False),
        args_parallel=dict(another_argument="something"),
    )
    job = await job_factory(
        project_id=project.id,
        dataset_id=dataset.id,
        workflow_id=workflow.id,
        working_dir="/foo",
        status="done",
    )

    # Run create_ome_zarr+yokogawa_to_zarr
    execute_tasks_mod(
        wf_task_list=[wftask0],
        dataset=dataset,
        workflow_dir_local=tmp_path / "job0",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset.id)

    # Run illumination_correction_compound
    dataset_with_attrs = await dataset_factory(
        project_id=project.id, zarr_dir=zarr_dir, **dataset_attrs
    )
    execute_tasks_mod(
        wf_task_list=[wftask1],
        dataset=dataset_with_attrs,
        workflow_dir_local=tmp_path / "job1",
        user_id=user_id,
        runner=local_runner,
        job_id=job.id,
        resource_id=resource.id,
    )
    dataset_attrs = await _get_dataset_attrs(db, dataset_with_attrs.id)

    # Check that there are now 4 images
    assert len(dataset_attrs["images"]) == 4
