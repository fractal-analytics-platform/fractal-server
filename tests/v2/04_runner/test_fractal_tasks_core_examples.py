import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from devtools import debug
from fractal_tasks_core_mock import TASK_LIST

from fractal_server.app.runner.v2 import execute_tasks_v2
from fractal_server.app.runner.v2.models import Dataset
from fractal_server.app.runner.v2.models import WorkflowTask
from fractal_server.images import find_image_by_path
from fractal_server.images import SingleImage


@pytest.fixture()
def executor():
    with ThreadPoolExecutor() as e:
        yield e


def _assert_image_data_exist(image_list: list[dict]):
    for image in image_list:
        assert (Path(image.path) / "data").exists()


def image_data_exist_on_disk(image_list: list[SingleImage]):
    """
    Given an image list, check whether mock data were written to disk.
    """
    prefix = "[image_data_exist_on_disk]"
    all_images_have_data = True
    for image in image_list:
        if (Path(image.path) / "data").exists():
            print(f"{prefix} {image.path} contains data")
        else:
            print(f"{prefix} {image.path} does *not* contain data")
            all_images_have_data = False
    return all_images_have_data


def test_fractal_demos_01(tmp_path: Path, executor):
    """
    Mock of fractal-demos/examples/01.
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_compound"],
                args_non_parallel=dict(image_dir="/tmp/input_images"),
                args_parallel={},
            )
        ],
        dataset=Dataset(zarr_dir=zarr_dir),
        executor=executor,
    )

    assert dataset.history == [None]  # FIXME
    assert dataset.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": 3,
    }
    _assert_image_data_exist(dataset.images)
    debug(dataset)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
                args_parallel=dict(overwrite_input=True),
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    assert dataset.history == [
        None,  # FIXME
        "illumination_correction",
    ]
    assert dataset.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": 3,
        "illumination_correction": True,
    }
    assert set(dataset.image_paths) == {
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    }

    img = find_image_by_path(
        path=f"{zarr_dir}/my_plate.zarr/A/01/0", images=dataset.images
    )
    assert img.dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }

    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["MIP_compound"],
                args_non_parallel=dict(suffix="mip"),
                args_parallel={},
            )
        ],
        dataset=dataset,
        executor=executor,
    )
    debug(dataset)

    assert dataset.history == [
        None,
        "illumination_correction",
        None,
    ]

    assert dataset.filters == {
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": 2,
        "illumination_correction": True,
    }
    img = find_image_by_path(
        path=f"{zarr_dir}/my_plate_mip.zarr/A/01/0", images=dataset.images
    )
    assert img.dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }
    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["cellpose_segmentation"],
                args_parallel={},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    debug(dataset)

    assert dataset.history == [
        None,
        "illumination_correction",
        None,
        "cellpose_segmentation",
    ]


def test_fractal_demos_01_no_overwrite(tmp_path: Path, executor):
    """
    Similar to fractal-demos/examples/01, but illumination
    correction task does not override its input images.
    """
    # The first block (up to yokogawa-to-zarr included) is identical to
    # the previous test
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
            )
        ],
        dataset=Dataset(zarr_dir=zarr_dir),
        executor=executor,
    )
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    ]
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["yokogawa_to_zarr"],
                args=dict(),
            )
        ],
        dataset=dataset,
        executor=executor,
    )
    debug(dataset)
    _assert_image_data_exist(dataset.images)

    # Run illumination correction with overwrite_input=False
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
                args=dict(overwrite_input=False),
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    assert dataset.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
    ]
    assert dataset.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": 3,
        "illumination_correction": True,
    }
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
    ]
    debug(dataset)

    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "attributes": {
            "well": "A02",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }
    assert dataset.images[2].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    assert dataset.images[3].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        "attributes": {
            "well": "A02",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args=dict(suffix="mip"),
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    assert dataset.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
        "new_ome_zarr",
    ]
    assert dataset.filters == {
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": 2,
        "illumination_correction": True,
    }
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
    ]

    assert dataset.images[4].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        "attributes": {
            "well": "A01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }
    assert dataset.images[5].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
        "attributes": {
            "well": "A02",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }
    # NOTE: new images do not exist yet on disk
    with pytest.raises(AssertionError):
        _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["maximum_intensity_projection"],
                args=dict(),
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    assert dataset.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
        "new_ome_zarr",
        "maximum_intensity_projection",
    ]
    assert dataset.filters == {
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": 2,
        "illumination_correction": True,
    }
    # Note: images now exist
    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["cellpose_segmentation"],
                args=dict(),
            )
        ],
        dataset=dataset,
        executor=executor,
    )
    debug(dataset)

    assert dataset.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
        "new_ome_zarr",
        "maximum_intensity_projection",
        "cellpose_segmentation",
    ]


@pytest.mark.skip("FIXME: update this")
def test_registration_old_overwrite(tmp_path: Path, executor):
    """
    TBD
    """
    # Setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run create-ome-zarr-multiplex
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_multiplex"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            )
        ],
        dataset=Dataset(),
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # We have 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # Image data do not exit on disk yet
    assert not image_data_exist_on_disk(dataset.images)

    # The first image looks like this
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }

    # Run yokogawa-to-zarr
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"]),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # We still have 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # The first-image metadata has not changed
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }

    # Image data now exist on disk
    assert image_data_exist_on_disk(dataset.images)

    # Run init-registration
    # NOTE: the reference cycle is currently identified by its name
    # (the last part of the OME-Zarr image path). We can then change
    # this into using some zarr metadata
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["init_registration"],
                args={"ref_cycle_name": "0"},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # The dataset now includes a custom parallelization list
    assert dataset.parallelization_list is not None

    # We still have the same 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # The first-image metadata has not changed
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }

    # Run registration
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["registration"],
                args={"overwrite_input": True},
            ),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # We still have the same 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # The first-image metadata (reference cycle) now has registration=True
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "registration": True,
        },
    }
    # The second-image metadata also have registration=True
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/1",
        "attributes": {
            "well": "A01",
            "acquisition": 1,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "registration": True,
        },
    }

    # The custom parallelization list is not present any more
    assert dataset.parallelization_list is None


@pytest.mark.skip("FIXME: update this")
def test_registration_old_no_overwrite(tmp_path: Path, executor):
    """
    TBD
    """
    # Setup
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run create-ome-zarr-multiplex
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_multiplex"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            )
        ],
        dataset=Dataset(),
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # We have 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # Image data do not exit on disk yet
    assert not image_data_exist_on_disk(dataset.images)

    # The first image looks like this
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }

    # Run yokogawa-to-zarr
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"]),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # We still have 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # The first-image metadata has not changed
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }

    # Image data now exist on disk
    assert image_data_exist_on_disk(dataset.images)

    # Run init-registration
    # NOTE: the reference cycle is currently identified by its name
    # (the last part of the OME-Zarr image path). We can then change
    # this into using some zarr metadata
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["init_registration"],
                args={"ref_cycle_name": "0"},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # The dataset now includes a custom parallelization list
    assert dataset.parallelization_list is not None

    # We still have the same 6 images (two wells, three cycles)
    assert len(dataset.images) == 6
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
    ]

    # The first-image metadata has not changed
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }

    # Run registration
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["registration"],
                args={"overwrite_input": False},
            ),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # We now have 12 images (6 raw and 6 registered)
    assert len(dataset.images) == 12
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/01/1",
        f"{zarr_dir}/my_plate.zarr/A/01/2",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/1",
        f"{zarr_dir}/my_plate.zarr/A/02/2",
        f"{zarr_dir}/my_plate.zarr/A/01/0_r",
        f"{zarr_dir}/my_plate.zarr/A/01/1_r",
        f"{zarr_dir}/my_plate.zarr/A/01/2_r",
        f"{zarr_dir}/my_plate.zarr/A/02/0_r",
        f"{zarr_dir}/my_plate.zarr/A/02/1_r",
        f"{zarr_dir}/my_plate.zarr/A/02/2_r",
    ]

    # The first-image metadata (reference cycle) has not changed
    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "acquisition": 0,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }
    # A registered image has the correct path and registration=True
    assert dataset.images[7].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/1_r",
        "attributes": {
            "well": "A01",
            "acquisition": 1,
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "registration": True,
        },
    }

    # The custom parallelization list is not present any more
    assert dataset.parallelization_list is None


def test_registration_no_overwrite(tmp_path: Path, executor):
    """
    Test registration workflow, based on four tasks.
    """

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_multiplex"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
        ],
        dataset=Dataset(zarr_dir=zarr_dir),
        executor=executor,
    )

    # Run init registration
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["init_registration"],
                args={"ref_acquisition": 0},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Run calculate registration
    dataset = execute_tasks_v2(
        wf_task_list=[WorkflowTask(task=TASK_LIST["calculate_registration"])],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # In all non-reference-cycle images, a certain table was updated
    for image in dataset.images:
        if image.attributes["acquisition"] == 0:
            assert not os.path.isfile(f"{image.path}/registration_table")
        else:
            assert os.path.isfile(f"{image.path}/registration_table")

    # Run find_registration_consensus
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(task=TASK_LIST["find_registration_consensus"])
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # In all images, a certain (post-consensus) table was updated
    for image in dataset.images:
        assert os.path.isfile(f"{image.path}/registration_table_final")

    # The image list still has the original six images only
    assert len(dataset.images) == 6

    # Run apply_registration_to_image
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["apply_registration_to_image"],
                args={"overwrite_input": False},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    # A new copy of each image was created
    assert len(dataset.images) == 12

    # Print current dataset information
    debug(dataset)


def test_registration_overwrite(tmp_path: Path, executor):
    """
    Test registration workflow, based on four tasks.
    """

    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_multiplex"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
        ],
        dataset=Dataset(zarr_dir=zarr_dir),
        executor=executor,
    )

    # Run init registration
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["init_registration"],
                args={"ref_acquisition": 0},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Run calculate registration
    dataset = execute_tasks_v2(
        wf_task_list=[WorkflowTask(task=TASK_LIST["calculate_registration"])],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # In all non-reference-cycle images, a certain table was updated
    for image in dataset.images:
        if image.attributes["acquisition"] == 0:
            assert not os.path.isfile(f"{image.path}/registration_table")
        else:
            assert os.path.isfile(f"{image.path}/registration_table")

    # Run find_registration_consensus
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(task=TASK_LIST["find_registration_consensus"])
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # In all images, a certain (post-consensus) table was updated
    for image in dataset.images:
        assert os.path.isfile(f"{image.path}/registration_table_final")

    # The image list still has the original six images only
    assert len(dataset.images) == 6

    # Run apply_registration_to_image
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["apply_registration_to_image"],
                args={"overwrite_input": True},
            )
        ],
        dataset=dataset,
        executor=executor,
    )

    # Images are still the same number, but they are marked as registered
    assert len(dataset.images) == 6
    for image in dataset.images:
        assert image.attributes["registration"] is True

    # Print current dataset information
    debug(dataset)


def test_channel_parallelization_with_overwrite(tmp_path: Path, executor):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run create_ome_zarr+yokogawa_to_zarr
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"]),
        ],
        dataset=Dataset(zarr_dir=zarr_dir),
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Run init_channel_parallelization
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["init_channel_parallelization"],
                args=dict(overwrite_input=True),
            ),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Look at custom parallelization_list
    debug(dataset.parallelization_list)
    assert dataset.parallelization_list is not None

    # Run init_channel_parallelization
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["illumination_correction_B"],
            ),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Parallelization list is not there any more
    assert dataset.parallelization_list is None

    # Print current dataset information
    debug(dataset)


def test_channel_parallelization_no_overwrite(tmp_path: Path, executor):
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

    # Run create_ome_zarr+yokogawa_to_zarr
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"]),
        ],
        dataset=Dataset(zarr_dir=zarr_dir),
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Run init_channel_parallelization
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["init_channel_parallelization"],
                args=dict(overwrite_input=False),
            ),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Look at custom parallelization_list
    debug(dataset.parallelization_list)
    assert dataset.parallelization_list is not None

    # Run init_channel_parallelization
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(task=TASK_LIST["illumination_correction_B"]),
        ],
        dataset=dataset,
        executor=executor,
    )

    # Print current dataset information
    debug(dataset)

    # Parallelization list is not there any more
    assert dataset.parallelization_list is None

    # Check that there are now 4 images
    assert len(dataset.images) == 4
