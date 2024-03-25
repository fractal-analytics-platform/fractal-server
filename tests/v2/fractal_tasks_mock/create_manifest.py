import json
import logging

from devtools import debug
from fractal_tasks_core.dev.lib_args_schemas import (
    create_schema_for_single_task,
)


TASK_LIST = [
    dict(
        name="create_ome_zarr_compound",
        executable_non_parallel="create_cellvoyager_ome_zarr.py",
        executable_parallel="fill_cellvoyager_ome_zarr.py",
    ),
    dict(
        name="create_ome_zarr_multiplex_compound",
        executable_non_parallel="create_cellvoyager_ome_zarr_multiplex.py",
        executable_parallel="fill_cellvoyager_ome_zarr.py",
    ),
    dict(
        name="MIP_compound",
        input_types={"3D": True},
        executable_non_parallel="new_ome_zarr.py",
        executable_parallel="maximum_intensity_projection.py",
        output_types={"3D": False},
    ),
    dict(
        name="illumination_correction",
        input_types=dict(illumination_correction=False),
        executable_parallel="illumination_correction.py",
        output_types=dict(illumination_correction=True),
    ),
    dict(
        name="illumination_correction_compound",
        input_types=dict(illumination_correction=False),
        executable_non_parallel="illumination_correction_init.py",
        executable_parallel="illumination_correction_compute.py",
        output_types=dict(illumination_correction=True),
    ),
    dict(
        name="cellpose_segmentation",
        executable_parallel="cellpose_segmentation.py",
    ),
    dict(
        name="calculate_registration_compound",
        executable_non_parallel="calculate_registration_init.py",
        executable_parallel="calculate_registration_compute.py",
    ),
    dict(
        name="find_registration_consensus",
        executable_non_parallel="find_registration_consensus.py",
    ),
    dict(
        name="apply_registration_to_image",
        input_types=dict(registration=False),
        executable_parallel="apply_registration_to_image.py",
        output_types=dict(registration=True),
    ),
]


PACKAGE = "fractal_tasks_mock"
CUSTOM_PYDANTIC_MODELS = [
    ("fractal_tasks_mock", "input_models.py", "InitArgsRegistration"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsCellVoyager"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsIllumination"),
    ("fractal_tasks_mock", "input_models.py", "InitArgsMIP"),
]


for ind, task in enumerate(TASK_LIST):
    print(task)

    for step in ["non_parallel", "parallel"]:

        key = f"executable_{step}"
        if key not in task.keys():
            continue

        executable = task[key]
        logging.info(f"[{executable}] START")

        # Create new JSON Schema for task arguments
        schema = create_schema_for_single_task(
            executable,
            package=PACKAGE,
            custom_pydantic_models=CUSTOM_PYDANTIC_MODELS,
        )

        TASK_LIST[ind][f"args_schema_{step}"] = schema

    # Update docs_info, based on task-function description
    TASK_LIST[ind]["docs_info"] = f"This is task {task['name']}."
    TASK_LIST[ind]["docs_link"] = "https://example.org"

    logging.info(f"[{executable}] END (new schema/description/link)")
    print()


manifest = dict(
    manifest_version="2",
    task_list=TASK_LIST,
    has_args_schemas=True,
    args_schema_version="pydantic_v1",
)
debug(manifest)

with open("src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json", "w") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
