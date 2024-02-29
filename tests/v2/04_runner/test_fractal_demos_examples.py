from pathlib import Path

import pytest
from devtools import debug
from fractal_tasks_core_mock import TASK_LIST

from fractal_server.images import find_image_by_path
from fractal_server.v2 import Dataset
from fractal_server.v2 import execute_tasks_v2
from fractal_server.v2 import WorkflowTask


def _assert_image_data_exist(image_list: list[dict]):
    for image in image_list:
        assert (Path(image.path) / "data").exists()


def test_fractal_demos_01(tmp_path: Path):
    """
    Mock of fractal-demos/examples/01.
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset = Dataset(id=1)
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            )
        ],
        dataset=dataset,
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
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "attributes": {
            "well": "A_02",
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
            "well": "A_01",
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
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "attributes": {
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    assert dataset.images[2].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
        "attributes": {
            "well": "A_01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }
    assert dataset.images[3].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0",
        "attributes": {
            "well": "A_02",
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
            "well": "A_01",
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
    dataset = Dataset(id=1)
    dataset = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            )
        ],
        dataset=dataset,
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
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }
    assert dataset.images[1].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
        "attributes": {
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
        },
    }
    assert dataset.images[2].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        "attributes": {
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": 3,
            "illumination_correction": True,
        },
    }
    assert dataset.images[3].dict() == {
        "path": f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        "attributes": {
            "well": "A_02",
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
            "well": "A_01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": 2,
            "illumination_correction": True,
        },
    }
    assert dataset.images[5].dict() == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
        "attributes": {
            "well": "A_02",
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
