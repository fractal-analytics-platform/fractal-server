from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.task_interface import TaskOutput


def merge_outputs(task_outputs: list[TaskOutput]) -> TaskOutput:

    final_image_list_updates = []
    final_image_list_removals = []

    for task_output in task_outputs:

        final_image_list_updates.extend(task_output.image_list_updates)
        final_image_list_removals.extend(task_output.image_list_removals)

        # Check that all type_filters are the same
        if task_output.type_filters != task_outputs[0].type_filters:
            raise ValueError(
                f"{task_output.type_filters=} "
                f"but {task_outputs[0].type_filters=}"
            )

    final_image_list_updates = deduplicate_list(final_image_list_updates)

    if task_outputs:
        final_output = TaskOutput(
            image_list_updates=final_image_list_updates,
            image_list_removals=final_image_list_removals,
            type_filters=task_outputs[0].type_filters,
        )
    else:
        final_output = TaskOutput(
            image_list_updates=final_image_list_updates,
            image_list_removals=final_image_list_removals,
        )

    return final_output
