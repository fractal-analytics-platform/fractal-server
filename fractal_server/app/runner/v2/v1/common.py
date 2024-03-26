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
from typing import Optional

from .....logger import close_logger as close_job_logger  # noqa F401


class TaskParameterEncoder(JSONEncoder):
    """
    Convenience JSONEncoder that serialises `Path`s as strings
    """

    def default(self, value):
        if isinstance(value, Path):
            return value.as_posix()
        return JSONEncoder.default(self, value)


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


def set_start_and_last_task_index(
    num_tasks: int,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
) -> tuple[int, int]:
    """
    Handle `first_task_index` and `last_task_index`, by setting defaults and
    validating values.

    num_tasks:
        Total number of tasks in a workflow task list
    first_task_index:
        Positional index of the first task to execute
    last_task_index:
        Positional index of the last task to execute
    """
    # Set default values
    if first_task_index is None:
        first_task_index = 0
    if last_task_index is None:
        last_task_index = num_tasks - 1

    # Perform checks
    if first_task_index < 0:
        raise ValueError(f"{first_task_index=} cannot be negative")
    if last_task_index < 0:
        raise ValueError(f"{last_task_index=} cannot be negative")
    if last_task_index > num_tasks - 1:
        raise ValueError(
            f"{last_task_index=} cannot be larger than {(num_tasks-1)=}"
        )
    if first_task_index > last_task_index:
        raise ValueError(
            f"{first_task_index=} cannot be larger than {last_task_index=}"
        )
    return (first_task_index, last_task_index)
