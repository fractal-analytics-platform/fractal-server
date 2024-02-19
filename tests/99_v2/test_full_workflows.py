from pathlib import Path

from devtools import debug

from fractal_server.v2 import Dataset
from fractal_server.v2 import execute_tasks_v2
from fractal_server.v2 import find_image_by_path
from fractal_server.v2 import TASK_LIST
from fractal_server.v2 import WorkflowTask


def _assert_image_data_exist(image_list: list[dict]):
    for image in image_list:
        assert (Path(image["path"]) / "data").exists()


def test_workflow_1(tmp_path: Path):
    """
    1. create-ome-zarr + yokogawa-to-zarr
    2. illumination correction (new images)
    3. new_ome_zarr + MIP
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
            ),
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args={"suffix": "mip"},
            ),
            WorkflowTask(
                task=TASK_LIST["maximum_intensity_projection"],
            ),
        ],
        dataset=dataset_in,
    )

    assert dataset_out.filters == {
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": "2",
        "illumination_correction": True,
    }

    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
        "new_ome_zarr",
        "maximum_intensity_projection",
    ]

    debug(dataset_out.images)
    assert set(dataset_out.image_paths) == {
        f"{zarr_dir}/my_plate.zarr/A/01/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0",
        f"{zarr_dir}/my_plate.zarr/A/02/0_corr",
        f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        f"{zarr_dir}/my_plate_mip.zarr/A/02/0_corr",
    }
    img = find_image_by_path(
        path=f"{zarr_dir}/my_plate.zarr/A/01/0_corr", images=dataset_out.images
    )
    assert img == {
        "path": f"{zarr_dir}/my_plate.zarr/A/01/0_corr",
        "well": "A_01",
        "plate": "my_plate.zarr",
        "data_dimensionality": "3",
        "illumination_correction": True,
    }

    img = find_image_by_path(
        path=f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        images=dataset_out.images,
    )
    assert img == {
        "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0_corr",
        "well": "A_01",
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": "2",
        "illumination_correction": True,
    }

    _assert_image_data_exist(dataset_out.images)


def test_workflow_2(tmp_path: Path):
    """
    1. create-ome-zarr + yokogawa-to-zarr
    2. illumination correction (overwrite_input=True)
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
                args=dict(overwrite_input=True),
            ),
        ],
        dataset=dataset_in,
    )

    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
    ]

    debug(dataset_out.filters)
    assert dataset_out.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": "3",
        "illumination_correction": True,
    }
    debug(dataset_out.images)
    assert dataset_out.images == [
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
            "illumination_correction": True,
        },
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
            "illumination_correction": True,
        },
    ]

    _assert_image_data_exist(dataset_out.images)


def test_workflow_3(tmp_path: Path):
    """
    1. create-ome-zarr + yokogawa-to-zarr
    2. illumination correction (overwrite_input=True) on a single well
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1, zarr_dir=zarr_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
                args=dict(overwrite_input=True),
                filters=dict(well="A_01"),
            ),
        ],
        dataset=dataset_in,
    )

    debug(dataset_out)
    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "illumination_correction",
    ]

    debug(dataset_out.filters)
    assert dataset_out.filters == {
        "plate": "my_plate.zarr",
        "data_dimensionality": "3",
        "illumination_correction": True,
    }
    debug(dataset_out.images)
    assert dataset_out.images == [
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
            "illumination_correction": True,
        },
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
    ]

    _assert_image_data_exist(dataset_out.images)


def test_workflow_4(tmp_path: Path):
    """
    1. create ome zarr + yokogawa-to-zarr
    2. new-ome-zarr + copy-data
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1, zarr_dir=zarr_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args={"suffix": "new", "project_to_2D": False},
            ),
            WorkflowTask(
                task=TASK_LIST["copy_data"],
            ),
        ],
        dataset=dataset_in,
    )

    debug(dataset_out)
    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "new_ome_zarr",
        "copy_data",
    ]

    debug(dataset_out.filters)
    assert dataset_out.filters == {
        "plate": "my_plate_new.zarr",
        "data_dimensionality": "3",
    }
    debug(dataset_out.images)
    assert dataset_out.images == [
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate_new.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate_new.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate_new.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate_new.zarr",
            "data_dimensionality": "3",
        },
    ]

    _assert_image_data_exist(dataset_out.images)


def test_workflow_5(tmp_path: Path):
    """
    1. create ome zarr + yokogawa-to-zarr
    2. new-ome-zarr + MIP
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1, zarr_dir=zarr_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args={"suffix": "mip"},
            ),
            WorkflowTask(
                task=TASK_LIST["maximum_intensity_projection"],
            ),
        ],
        dataset=dataset_in,
    )

    debug(dataset_out)
    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "new_ome_zarr",
        "maximum_intensity_projection",
    ]
    assert dataset_out.filters == {
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": "2",
    }
    assert dataset_out.images == [
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": "2",
        },
        {
            "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": "2",
        },
    ]
    _assert_image_data_exist(dataset_out.images)


def test_workflow_6(tmp_path: Path):
    """
    1. create ome zarr + yokogawa-to-zarr
    2. new-ome-zarr + MIP
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1, zarr_dir=zarr_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"]),
            WorkflowTask(task=TASK_LIST["init_channel_parallelization"]),
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
            ),
        ],
        dataset=dataset_in,
    )

    debug(dataset_out)
    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "init_channel_parallelization",
        "illumination_correction",
    ]

    _assert_image_data_exist(dataset_out.images)


def test_workflow_7(tmp_path: Path):
    """
    1. create ome zarr multiplex + yokogawa-to-zarr
    2. init_registration
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1, zarr_dir=zarr_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_multiplex"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["init_registration"],
                args={"ref_cycle_name": "0"},
            ),
        ],
        dataset=dataset_in,
    )

    debug(dataset_out)
    assert dataset_out.history == [
        "create_ome_zarr_multiplex",
        "yokogawa_to_zarr",
        "init_registration",
    ]

    _assert_image_data_exist(dataset_out.images)


def test_workflow_8(tmp_path: Path):
    """
    1. create ome zarr + yokogawa-to-zarr
    2. new_ome_zarr + MIP
    3. cellpose segmentation for 3D data
    4. cellpose segmentation for 2D data
    """
    zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")
    dataset_in = Dataset(id=1, zarr_dir=zarr_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images", zarr_dir=zarr_dir),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args={"suffix": "mip"},
            ),
            WorkflowTask(
                task=TASK_LIST["maximum_intensity_projection"],
            ),
            WorkflowTask(
                task=TASK_LIST["cellpose_segmentation"],
            ),
            WorkflowTask(
                task=TASK_LIST["cellpose_segmentation"],
                filters=dict(data_dimensionality="3", plate=None),
            ),
        ],
        dataset=dataset_in,
    )

    debug(dataset_out)
    assert dataset_out.history == [
        "create_ome_zarr",
        "yokogawa_to_zarr",
        "new_ome_zarr",
        "maximum_intensity_projection",
        "cellpose_segmentation",
        "cellpose_segmentation",
    ]
    assert dataset_out.images == [
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
        {
            "path": f"{zarr_dir}/my_plate_mip.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": "2",
        },
        {
            "path": f"{zarr_dir}/my_plate_mip.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate_mip.zarr",
            "data_dimensionality": "2",
        },
    ]
    _assert_image_data_exist(dataset_out.images)

    # In this workflow, cellpose should have run on both 3D and 2D data
    for image_path in dataset_out.image_paths:
        with (Path(image_path) / "data").open("r") as f:
            log = f.read()
        assert "Cellpose" in log
