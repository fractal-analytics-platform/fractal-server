from typing import Optional


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
