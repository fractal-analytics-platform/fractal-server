from .models import Workflow
from .models import WorkflowTask
from .tasks import TASK_LIST

WORKFLOWS = [
    Workflow(
        task_list=[
            WorkflowTask(
                task=TASK_LIST["create_ome_zarr"],
                args=dict(image_dir="/tmp/input_images"),
            ),
            WorkflowTask(task=TASK_LIST["yokogawa_to_zarr"], args={}),
            WorkflowTask(
                task=TASK_LIST["illumination_correction"],
                args={},
            ),
            WorkflowTask(
                task=TASK_LIST["cellpose_segmentation"],
                args={},
            ),
            WorkflowTask(
                task=TASK_LIST["new_ome_zarr"],
                args={"suffix": "mip"},
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
                task=TASK_LIST["illumination_correction"],
                args=dict(overwrite_input=True),
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
                task=TASK_LIST["illumination_correction"],
                args=dict(overwrite_input=True),
                filters=dict(well="A_01"),
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
