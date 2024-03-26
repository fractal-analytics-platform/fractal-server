from copy import copy

from fractal_server.app.runner.v2.deduplicate_list import deduplicate_list
from fractal_server.app.runner.v2.task_interface import TaskOutput
from fractal_server.images import SingleImage


def merge_outputs(task_outputs: list[TaskOutput]) -> TaskOutput:

    final_image_list_updates = []
    final_image_list_removals = []

    for ind, task_output in enumerate(task_outputs):

        final_image_list_updates.extend(task_output.image_list_updates)
        final_image_list_removals.extend(task_output.image_list_removals)

        # Check that all filters are the same
        current_new_filters = task_output.filters
        if ind == 0:
            last_new_filters = copy(current_new_filters)
        if current_new_filters != last_new_filters:
            raise ValueError(f"{current_new_filters=} but {last_new_filters=}")
        last_new_filters = copy(current_new_filters)

    final_image_list_updates = deduplicate_list(
        final_image_list_updates, PydanticModel=SingleImage
    )

    final_output = TaskOutput(
        image_list_updates=final_image_list_updates,
        image_list_removals=final_image_list_removals,
        filters=last_new_filters,
    )

    return final_output
