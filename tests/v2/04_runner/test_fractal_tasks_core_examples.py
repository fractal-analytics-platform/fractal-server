from pathlib import Path

import pytest
from devtools import debug
from fractal_tasks_core_mock import TASK_LIST

from fractal_server.app.runner.v2 import execute_tasks_v2
from fractal_server.app.runner.v2.models import Dataset
from fractal_server.app.runner.v2.models import WorkflowTask
from fractal_server.images import find_image_by_path
from fractal_server.images import SingleImage


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


def test_fractal_demos_01(tmp_path: Path):
    """
    Mock of fractal-demos/examples/01.
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            )
        ],
        dataset=Dataset(),
    )

    assert dataset.history == [
        "create_ome_zarr",
    ]
    assert dataset.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": 3,
    }
    assert dataset.image_paths == [
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
    ]
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

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["yokogawa_to_zarr"],
                args=dict(),
            )
        ],
        dataset=dataset,
    )

    assert dataset.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
    ]
    assert dataset.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": 3,
    }
    _assert_image_data_exist(dataset.images)

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
                args=dict(overwrite_input=True),
            )
        ],
        dataset=dataset,
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
                task=TASK_LIST["new_ome_zarr"],
                args=dict(suffix="mip", project_to_2D=True),
            )
        ],
        dataset=dataset,
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
    assert set(dataset.image_paths) == {
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
        f"{zarr_dir}/my_plate_mip.zarr/A/02/0",
    }
    _assert_image_data_exist(
        [
            image
            for image in dataset.images
            if image.attributes.get("data_dimensionality") == 3
        ]
    )

    with pytest.raises(AssertionError):
        _assert_image_data_exist(
            [
                image
                for image in dataset.images
                if image.attributes.get("data_dimensionality") == 2
            ]
        )

    assert dataset.images[0].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "attributes": {
            "well": "A02",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    assert dataset.images[2].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
        "attributes": {
            "well": "A01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }
    assert dataset.images[3].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0",
        "attributes": {
            "well": "A02",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }

    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["maximum_intensity_projection"],
                args=dict(),
            )
        ],
        dataset=dataset,
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
                args=dict(),
            )
        ],
        dataset=dataset,
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


def test_fractal_demos_01_no_overwrite(tmp_path: Path):
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
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            )
        ],
        dataset=Dataset(),
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


def test_example_registration(tmp_path: Path):
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

    # Image data now exist on disk yet
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
            ),
        ],
        dataset=dataset,
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
