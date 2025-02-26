import json
import logging

from fractal_tasks_core.dev.lib_args_schemas import (
    create_schema_for_single_task,
)
from task_models import CompoundTask
from task_models import NonParallelTask
from task_models import ParallelTask


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
        input_types={"__no_images__": True},
    ),
    CompoundTask(
        name="create_ome_zarr_multiplex_compound",
        executable_init="create_cellvoyager_ome_zarr_multiplex.py",
        executable="fill_cellvoyager_ome_zarr.py",
        category="Conversion",
        modality="HCS",
        tags=["Yokogawa", "Cellvoyager"],
        input_types={"__no_images__": True},
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
CUSTOM_PYDANTIC_MODELS = [
    ("fractal_tasks_mock", "input_models.py", "InitArgsRegistration"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsCellVoyager"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsIllumination"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsMIP"),
]


for ind, task in enumerate(TASK_LIST):
    TASK_LIST[ind] = TASK_LIST[ind].dict(
        exclude={"meta_init", "executable_init", "meta", "executable"},
        exclude_unset=True,
    )

    if task.executable_non_parallel is not None:
        TASK_LIST[ind][
            "executable_non_parallel"
        ] = task.executable_non_parallel
    if task.executable_parallel is not None:
        TASK_LIST[ind]["executable_parallel"] = task.executable_parallel

    for step in ["non_parallel", "parallel"]:
        executable = TASK_LIST[ind].get(f"executable_{step}")
        if executable is None:
            continue

        # Create new JSON Schema for task arguments
        logging.info(f"[{executable}] START")
        schema = create_schema_for_single_task(
            executable,
            package=PACKAGE,
            custom_pydantic_models=CUSTOM_PYDANTIC_MODELS,
        )
        logging.info(f"[{executable}] END (new schema)")

        TASK_LIST[ind][f"args_schema_{step}"] = schema

    if task.meta_non_parallel is not None:
        TASK_LIST[ind]["meta_non_parallel"] = task.meta_non_parallel
    if task.meta_parallel is not None:
        TASK_LIST[ind]["meta_parallel"] = task.meta_parallel

    # Update docs_info, based on task-function description
    TASK_LIST[ind]["docs_info"] = f"This is task {task.name}."
    TASK_LIST[ind]["docs_link"] = "https://example.org"


manifest = dict(
    manifest_version="2",
    task_list=TASK_LIST,
    has_args_schemas=True,
    args_schema_version="pydantic_v1",
    authors=AUTHORS,
)

with open("src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json", "w") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
