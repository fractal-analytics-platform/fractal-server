from copy import copy
from copy import deepcopy

from .filters import filter_images
from .filters import FilterSet
from .images import _deduplicate_list_of_dicts
from .images import find_image_by_path
from .images import SingleImage
from .models import Dataset
from .models import KwargsType
from .models import WorkflowTask
from .runner_functions import _run_non_parallel_task
from .runner_functions import _run_parallel_task
from .task_output import TaskOutput
from .utils import ipjson


# FIXME: define RESERVED_ARGUMENTS = ["buffer", ...]


def _apply_attributes_to_image(
    *,
    image: SingleImage,
    filters: FilterSet,
) -> SingleImage:
    updated_image = copy(image)
    for key, value in filters.items():
        updated_image[key] = value
    return updated_image


def _validate_parallelization_list_valid(
    parallelization_list: list[KwargsType],
    current_image_paths: list[SingleImage],
) -> None:
    for kwargs in parallelization_list:
        path = kwargs.get("path")
        if path is None:
            raise ValueError(
                "An element in parallelization list has no path:\n"
                f"{kwargs=}"
            )
        if path not in current_image_paths:
            raise ValueError(
                "An element in parallelization list does not match "
                f"with any image:\n{kwargs=}"
            )
        if "buffer" in kwargs.keys():
            raise ValueError(
                f"An element in parallelization list is not valid:\n{kwargs=}"
            )


def execute_tasks_v2(
    wf_task_list: list[WorkflowTask],
    dataset: Dataset,
) -> Dataset:

    # Run task 0
    tmp_dataset = deepcopy(dataset)

    for wftask in wf_task_list:
        task = wftask.task

        print(f"NOW RUN {task.name} (task type: {task.task_type})")

        # Extract tmp_buffer
        if tmp_dataset.buffer is not None:
            tmp_buffer = tmp_dataset.buffer
        else:
            tmp_buffer = {}

        # Extract parallelization_list
        if tmp_dataset.parallelization_list is not None:
            parallelization_list = tmp_dataset.parallelization_list
            parallelization_list = _deduplicate_list_of_dicts(
                parallelization_list
            )
            _validate_parallelization_list_valid(
                parallelization_list=parallelization_list,
                current_image_paths=tmp_dataset.image_paths,
            )
        else:
            parallelization_list = None

        # (1/2) Non-parallel task
        if task.task_type == "non_parallel":
            if parallelization_list is not None:
                raise ValueError(
                    "Found parallelization_list for non-parallel task"
                )
            else:
                # Get filtered images
                filtered_images = filter_images(
                    dataset_images=tmp_dataset.images,
                    dataset_filters=tmp_dataset.filters,
                    wftask_filters=wftask.filters,
                )
                paths = [image["path"] for image in filtered_images]
                function_kwargs = dict(
                    paths=paths,
                    # root_dir=tmp_dataset.root_dir,
                    buffer=tmp_buffer,
                    **wftask.args,
                )
                task_output = _run_non_parallel_task(
                    task=task,
                    function_kwargs=function_kwargs,
                    old_dataset_images=filtered_images,
                )
        # (2/2) Parallel task
        elif task.task_type == "parallel":
            # Prepare list_function_kwargs
            if parallelization_list is None:
                # Get filtered images
                filtered_images = filter_images(
                    dataset_images=tmp_dataset.images,
                    dataset_filters=tmp_dataset.filters,
                    wftask_filters=wftask.filters,
                )
                list_function_kwargs = []
                for image in filtered_images:
                    list_function_kwargs.append(
                        dict(
                            path=image["path"],
                            # root_dir=tmp_dataset.root_dir,
                            buffer=tmp_buffer,
                            **wftask.args,
                        )
                    )
            else:
                # Use pre-made parallelization_list
                list_function_kwargs = deepcopy(parallelization_list)
                for ind, kwargs in enumerate(list_function_kwargs):
                    if "buffer" in kwargs:
                        raise ValueError(f"Invalid {kwargs=}")
                    list_function_kwargs[ind].update(
                        dict(
                            # root_dir=tmp_dataset.root_dir,
                            buffer=tmp_buffer,
                            **wftask.args,
                        )
                    )
                # TODO: can we avoid this deduplicate operation?
                list_function_kwargs = _deduplicate_list_of_dicts(
                    list_function_kwargs
                )

                filtered_images = [
                    find_image_by_path(
                        images=tmp_dataset.images, path=kwargs["path"]
                    )
                    for kwargs in list_function_kwargs
                ]  # FIXME change name `filtered_images`
            task_output = _run_parallel_task(
                task=task,
                list_function_kwargs=list_function_kwargs,
                old_dataset_images=filtered_images,
            )
        else:
            raise ValueError(f"Invalid {task.task_type=}.")

        # Redundant validation step (useful especially to check the merged
        # output of a parallel task)
        TaskOutput(**task_output)

        # Decorate new images with source-image attributes
        new_images = task_output.get("new_images", [])

        # Construct up-to-date filters
        new_filters = copy(tmp_dataset.filters)
        new_filters.update(task.new_filters)
        actual_task_new_filters = task_output.get("new_filters", {})
        new_filters.update(actual_task_new_filters)
        print(f"Dataset old filters:\n{ipjson(tmp_dataset.filters)}")
        print(f"Task.new_filters:\n{ipjson(task.new_filters)}")
        print(
            f"Actual new filters from task:\n{ipjson(actual_task_new_filters)}"
        )
        print(f"Combined new filters:\n{ipjson(new_filters)}")

        # Add filters to edited images, and update Dataset.images
        edited_images = task_output.get("edited_images", [])
        edited_paths = [image["path"] for image in edited_images]
        for ind, image in enumerate(tmp_dataset.images):
            if image["path"] in edited_paths:
                updated_image = _apply_attributes_to_image(
                    image=image, filters=new_filters
                )
                tmp_dataset.images[ind] = updated_image
        # Add filters to new images
        new_images = task_output.get("new_images", [])
        for ind, image in enumerate(new_images):
            updated_image = _apply_attributes_to_image(
                image=image, filters=new_filters
            )
            new_images[ind] = updated_image
        new_images = _deduplicate_list_of_dicts(new_images)

        # Get removed images
        removed_images = task_output.get("removed_images", [])

        # Add new images to Dataset.images
        for image in new_images:
            try:
                overlap = next(
                    _image
                    for _image in tmp_dataset.images
                    if _image["path"] == image["path"]
                )
                raise ValueError(f"Found {overlap=}")
            except StopIteration:
                pass
            print(f"Add {image} to list")
            tmp_dataset.images.append(image)

        # Remove images from Dataset.images
        removed_images_paths = [
            removed_image["path"] for removed_image in removed_images
        ]

        tmp_dataset.images = [
            image
            for image in tmp_dataset.images
            if image["path"] not in removed_images_paths
        ]
        # Update Dataset.filters
        tmp_dataset.filters = new_filters

        # Update Dataset.buffer
        tmp_dataset.buffer = task_output.get("buffer")

        # Update Dataset.parallelization_list
        # NOTE: this mut be done *after* Dataset.images was updated
        new_parallelization_list = task_output.get("parallelization_list")
        if new_parallelization_list is not None:
            _validate_parallelization_list_valid(
                parallelization_list=new_parallelization_list,
                current_image_paths=tmp_dataset.image_paths,
            )
        tmp_dataset.parallelization_list = new_parallelization_list

        # Update Dataset.history
        tmp_dataset.history.append(task.name)

        # End-of-task logs
        print(f"AFTER RUNNING {task.name}, we have this dataset:")
        print(ipjson(tmp_dataset.dict()))
        print("\n" + "-" * 88 + "\n")

    return tmp_dataset
