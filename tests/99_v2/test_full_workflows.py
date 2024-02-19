from pathlib import Path

import pytest
from devtools import debug

from fractal_server.v2 import Dataset
from fractal_server.v2 import execute_tasks_v2
from fractal_server.v2 import find_image_by_path
from fractal_server.v2 import TASK_LIST
from fractal_server.v2 import Workflow
from fractal_server.v2 import WorkflowTask


def test_workflow_1(tmp_path: Path):
    """
    1. create-ome-zarr + yokogawa-to-zarr
    2. illumination correction (new images)
    3. new_ome_zarr + MIP
    """
    root_dir = (tmp_path / "root_dir").as_posix()
    dataset_in = Dataset(id=1, root_dir=root_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
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
        "my_plate.zarr/A/01/0",
        "my_plate.zarr/A/02/0",
        "my_plate.zarr/A/02/0_corr",
        "my_plate.zarr/A/01/0_corr",
        "my_plate_mip.zarr/A/01/0_corr",
        "my_plate_mip.zarr/A/02/0_corr",
    }
    img = find_image_by_path(
        path="my_plate.zarr/A/01/0_corr", images=dataset_out.images
    )
    assert img == {
        "path": "my_plate.zarr/A/01/0_corr",
        "well": "A_01",
        "plate": "my_plate.zarr",
        "data_dimensionality": "3",
        "illumination_correction": True,
    }

    img = find_image_by_path(
        path="my_plate_mip.zarr/A/01/0_corr", images=dataset_out.images
    )
    assert img == {
        "path": "my_plate_mip.zarr/A/01/0_corr",
        "well": "A_01",
        "plate": "my_plate_mip.zarr",
        "data_dimensionality": "2",
        "illumination_correction": True,
    }


def test_workflow_2(tmp_path: Path):
    """
    1. create-ome-zarr + yokogawa-to-zarr
    2. illumination correction (overwrite_input=True)
    """
    root_dir = (tmp_path / "root_dir").as_posix()
    dataset_in = Dataset(id=1, root_dir=root_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
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
            "path": "my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
            "illumination_correction": True,
        },
        {
            "path": "my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
            "illumination_correction": True,
        },
    ]


def test_workflow_3(tmp_path: Path):
    """
    1. create-ome-zarr + yokogawa-to-zarr
    2. illumination correction (overwrite_input=True) on a single well
    """
    root_dir = (tmp_path / "root_dir").as_posix()
    dataset_in = Dataset(id=1, root_dir=root_dir)
    dataset_out = execute_tasks_v2(
        wf_task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
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
            "path": "my_plate.zarr/A/01/0",
            "well": "A_01",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
            "illumination_correction": True,
        },
        {
            "path": "my_plate.zarr/A/02/0",
            "well": "A_02",
            "plate": "my_plate.zarr",
            "data_dimensionality": "3",
        },
    ]


WORKFLOWS = [
    Workflow(
        task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args={"suffix": "new"},
            ),
            WorkflowTask(
                task=TASK_LIST["copy_data"],
            ),
        ],
    ),
    Workflow(
        task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
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
    ),
    Workflow(
        task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"]),
            WorkflowTask(task=TASK_LIST["init_channel_parallelization"]),
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
            ),
        ],
    ),
    Workflow(
        task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr_multiplex"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["init_registration"],
                args={"ref_cycle_name": "0"},
            ),
        ],
    ),
    Workflow(
        task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
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
    ),
]


@pytest.mark.skip()
@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_full_workflows(workflow: Workflow, tmp_path: Path):
    root_dir = (tmp_path / "root_dir").as_posix()
    dataset = Dataset(id=1, root_dir=root_dir)
    execute_tasks_v2(wf_task_list=workflow.task_list, dataset=dataset)
