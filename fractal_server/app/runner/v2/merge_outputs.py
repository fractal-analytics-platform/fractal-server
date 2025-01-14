from copy import copy

from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.task_interface import TaskOutput


def merge_outputs(task_outputs: list[TaskOutput]) -> TaskOutput:

    final_image_list_updates = []
    final_image_list_removals = []
    last_new_type_filters = None

    for ind, task_output in enumerate(task_outputs):

        final_image_list_updates.extend(task_output.image_list_updates)
        final_image_list_removals.extend(task_output.image_list_removals)

        # Check that all filters are the same
        current_new_type_filters = task_output.type_filters
        if ind == 0:
            last_new_type_filters = copy(current_new_type_filters)
        if current_new_type_filters != last_new_type_filters:
            raise ValueError(
                f"{current_new_type_filters=} but {last_new_type_filters=}"
            )
        last_new_type_filters = copy(current_new_type_filters)

    final_image_list_updates = deduplicate_list(final_image_list_updates)

    if last_new_type_filters is None:
        final_output = TaskOutput(
            image_list_updates=final_image_list_updates,
            image_list_removals=final_image_list_removals,
        )
    else:
        final_output = TaskOutput(
            image_list_updates=final_image_list_updates,
            image_list_removals=final_image_list_removals,
            type_filters=last_new_type_filters,
        )

    return final_output
