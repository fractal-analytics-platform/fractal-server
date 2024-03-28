"""
Common utilities and routines for runner backends (public API)

This module includes utilities and routines that are of use to implement
runner backends but that should also be exposed to the other components of
`Fractal Server`.
"""
import json
from json import JSONEncoder
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from ....logger import close_logger as close_job_logger  # noqa F401
from ...models.v1 import Dataset
from ...models.v1 import Workflow


class TaskParameterEncoder(JSONEncoder):
    """
    Convenience JSONEncoder that serialises `Path`s as strings
    """

    def default(self, value):
        if isinstance(value, Path):
            return value.as_posix()
        return JSONEncoder.default(self, value)


class TaskParameters(BaseModel):
    """
    Wrapper for task input parameters

    Instances of this class are used to pass parameters from the output of a
    task to the input of the next one.

    Attributes:
        input_paths:
            Input paths as derived by the input dataset.
        output_paths:
            Output path as derived from the output dataset.
        metadata:
            Dataset metadata, as found in the input dataset or as updated by
            the previous task.
        history:
            Dataset history, as found in the input dataset or as updated by
            the previous task.
    """

    input_paths: list[Path]
    output_path: Path
    metadata: dict[str, Any]
    history: list[dict[str, Any]]

    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"


def validate_workflow_compatibility(
    *,
    input_dataset: Dataset,
    workflow: Workflow,
    output_dataset: Dataset,
    first_task_index: int,
    last_task_index: int,
) -> None:
    """
    Check compatibility of workflow and input / ouptut dataset
    """
    # Check input_dataset type
    workflow_input_type = workflow.task_list[first_task_index].task.input_type
    if (
        workflow_input_type != "Any"
        and workflow_input_type != input_dataset.type
    ):
        raise TypeError(
            f"Incompatible types `{workflow_input_type}` of workflow "
            f"`{workflow.name}` and `{input_dataset.type}` of dataset "
            f"`{input_dataset.name}`"
        )

    # Check output_dataset type
    workflow_output_type = workflow.task_list[last_task_index].task.output_type
    if (
        workflow_output_type != "Any"
        and workflow_output_type != output_dataset.type
    ):
        raise TypeError(
            f"Incompatible types `{workflow_output_type}` of workflow "
            f"`{workflow.name}` and `{output_dataset.type}` of dataset "
            f"`{output_dataset.name}`"
        )


def write_args_file(
    *args: dict[str, Any],
    path: Path,
):
    """
    Merge arbitrary dictionaries and write to file

    Args:
        *args:
            One or more dictionaries that will be merged into one respecting
            the order with which they are passed in, i.e., last in overrides
            previous ones.
        path:
            Destination for serialised file.
    """
    out = {}
    for d in args:
        out.update(d)

    with open(path, "w") as f:
        json.dump(out, f, cls=TaskParameterEncoder, indent=4)
