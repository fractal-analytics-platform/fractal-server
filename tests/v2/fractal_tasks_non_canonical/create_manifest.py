import json
import logging

from fractal_tasks_core.dev.lib_args_schemas import (
    create_schema_for_single_task,
)
from task_models import NonParallelTask
from task_models import ParallelTask


TASK_LIST = [
    NonParallelTask(
        name="generic_task",
        executable="generic_task.py",
    ),
    ParallelTask(
        name="generic_task_parallel",
        executable="generic_task_parallel.py",
        input_types=dict(my_type=False),
        output_types=dict(my_type=True),
    ),
]


PACKAGE = "fractal_tasks_non_canonical"
CUSTOM_PYDANTIC_MODELS = []


for ind, task in enumerate(TASK_LIST):
    TASK_LIST[ind] = TASK_LIST[ind].model_dump(
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
)

with open(
    "src/fractal_tasks_non_canonical/__FRACTAL_MANIFEST__.json", "w"
) as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
