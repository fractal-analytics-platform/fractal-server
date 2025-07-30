from typing import Any


def get_new_workflow_task_meta(
    *,
    old_workflow_task_meta: dict | None,
    old_task_meta: dict | None,
    new_task_meta: dict | None,
) -> dict[str, Any] | None:
    """
    Prepare new meta field based on old/new tasks and old workflow task.
    """

    # When the whole `old_workflow_task_meta` is user-provided, use it
    # as the outcome
    if old_task_meta is None:
        return old_workflow_task_meta

    # When `old_workflow_task_meta` is unset, use the new-task meta as default.
    if old_workflow_task_meta is None:
        return new_task_meta

    if new_task_meta is None:
        new_task_meta = {}

    # Find properties that were added to the old defaults
    additions = {
        k: v
        for k, v in old_workflow_task_meta.items()
        if v != old_task_meta.get(k)
    }
    # Find properties that were removed from the old defaults
    removals = old_task_meta.keys() - old_workflow_task_meta.keys()

    # Add `additions` and remove `removals`.
    new_workflowtask_meta = {
        k: v
        for k, v in (new_task_meta | additions).items()
        if k not in removals
    }

    return new_workflowtask_meta
