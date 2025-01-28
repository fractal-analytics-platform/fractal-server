from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.task_interface import TaskOutput


def merge_outputs(task_outputs: list[TaskOutput]) -> TaskOutput:

    if len(task_outputs) == 0:
        return TaskOutput()

    final_image_list_updates = []
    final_image_list_removals = []

    for task_output in task_outputs:

        final_image_list_updates.extend(task_output.image_list_updates)
        final_image_list_removals.extend(task_output.image_list_removals)

    # Note: the ordering of `image_list_removals` is not guaranteed
    final_image_list_updates = deduplicate_list(final_image_list_updates)
    final_image_list_removals = list(set(final_image_list_removals))

    final_output = TaskOutput(
        image_list_updates=final_image_list_updates,
        image_list_removals=final_image_list_removals,
    )

    return final_output
