from fractal_task_tools.task_models import CompoundTask
from fractal_task_tools.task_models import NonParallelTask
from fractal_task_tools.task_models import ParallelTask


TASK_LIST = [
    CompoundTask(
        name="create_ome_zarr_compound",
        executable_init="create_cellvoyager_ome_zarr.py",
        executable="fill_cellvoyager_ome_zarr.py",
        meta_init={"key1": "value1"},
        meta={"key2": "value2"},
        category="Conversion",
        modality="HCS",
        tags=["Yokogawa", "Cellvoyager"],
    ),
    CompoundTask(
        name="create_ome_zarr_multiplex_compound",
        executable_init="create_cellvoyager_ome_zarr_multiplex.py",
        executable="fill_cellvoyager_ome_zarr.py",
        category="Conversion",
        modality="HCS",
        tags=["Yokogawa", "Cellvoyager"],
    ),
    CompoundTask(
        name="MIP_compound",
        input_types={"3D": True},
        executable_init="new_ome_zarr.py",
        executable="maximum_intensity_projection.py",
        output_types={"3D": False},
        category="Image Processing",
        modality="HCS",
        tags=["Preprocessing"],
    ),
    ParallelTask(
        name="illumination_correction",
        input_types=dict(illumination_correction=False),
        executable="illumination_correction.py",
        output_types=dict(illumination_correction=True),
        category="Image Processing",
        tags=["Preprocessing"],
    ),
    CompoundTask(
        name="illumination_correction_compound",
        input_types=dict(illumination_correction=False),
        executable_init="illumination_correction_init.py",
        executable="illumination_correction_compute.py",
        output_types=dict(illumination_correction=True),
        category="Image Processing",
        tags=["Preprocessing"],
    ),
    ParallelTask(
        name="cellpose_segmentation",
        executable="cellpose_segmentation.py",
        category="Segmentation",
        tags=[
            "Deep Learning",
            "Convolutional Neural Network",
            "Instance Segmentation",
        ],
    ),
    CompoundTask(
        name="calculate_registration_compound",
        executable_init="calculate_registration_init.py",
        executable="calculate_registration_compute.py",
        category="Registration",
        modality="HCS",
        tags=["Multiplexing"],
    ),
    NonParallelTask(
        name="find_registration_consensus",
        executable="find_registration_consensus.py",
        category="Registration",
        modality="HCS",
        tags=["Multiplexing"],
    ),
    ParallelTask(
        name="apply_registration_to_image",
        input_types=dict(registration=False),
        executable="apply_registration_to_image.py",
        output_types=dict(registration=True),
        category="Registration",
        modality="HCS",
        tags=["Multiplexing"],
    ),
    NonParallelTask(
        name="generic_task",
        executable="generic_task.py",
    ),
    NonParallelTask(
        name="dummy_remove_images", executable="dummy_remove_images.py"
    ),
    NonParallelTask(
        name="dummy_insert_single_image",
        executable="dummy_insert_single_image.py",
    ),
    NonParallelTask(
        name="dummy_unset_attribute",
        executable="dummy_unset_attribute.py",
    ),
    ParallelTask(
        name="generic_task_parallel",
        executable="generic_task_parallel.py",
        input_types=dict(my_type=False),
        output_types=dict(my_type=True),
    ),
]


PACKAGE = "fractal_tasks_mock"
AUTHORS = "name1 surname1, name2 surname2"
INPUT_MODELS = [
    ("fractal_tasks_mock", "input_models.py", "InitArgsRegistration"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsCellVoyager"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsIllumination"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsMIP"),
]
