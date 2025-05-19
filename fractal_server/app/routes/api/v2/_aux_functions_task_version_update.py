def get_new_workflow_task_meta(
    *,
    old_task_meta: dict | None,
    old_workflow_task_meta: dict | None,
    new_task_meta: dict | None,
) -> dict:

    if old_task_meta is None:
        # all the contents of old_workflow_task_meta are user-made,
        # and this should be the output.
        return old_workflow_task_meta

    if old_workflow_task_meta is None:
        return new_task_meta

    if new_task_meta is None:
        new_task_meta = {}

    additions = {
        k: v
        for k, v in old_workflow_task_meta.items()
        if v != old_task_meta.get(k)
    }
    removals = old_task_meta.keys() - old_workflow_task_meta.keys()

    new_workflowtask_meta = {
        k: v
        for k, v in (new_task_meta | additions).items()
        if k not in removals
    }

    return new_workflowtask_meta
