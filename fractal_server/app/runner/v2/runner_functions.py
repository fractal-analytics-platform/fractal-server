from concurrent.futures import ThreadPoolExecutor
from copy import copy
from typing import Any
from typing import TypeVar

from pydantic import BaseModel
from pydantic import Field
from pydantic.main import ModelMetaclass

from ....images import SingleImage
from .models import DictStrAny
from .models import Task
from .models import TaskV1
from .models import WorkflowTask
from .task_output import TaskOutput


MAX_PARALLELIZATION_LIST_SIZE = 200


def _run_non_parallel_task(
    *,
    filtered_images: list[SingleImage],
    zarr_dir: str,
    task: Task,
    wftask: WorkflowTask,
    executor,
) -> TaskOutput:

    paths = [image.path for image in filtered_images]
    function_kwargs = dict(
        paths=paths,
        zarr_dir=zarr_dir,
        **wftask.args_non_parallel,
    )

    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_non_parallel(**input_kwargs)
        if task_output is None:
            task_output = TaskOutput()
        else:
            task_output = TaskOutput(**task_output)
        return task_output

    task_output = executor.submit(
        _wrapper_expand_kwargs, function_kwargs
    ).result()

    return TaskOutput(**task_output.dict())


class InitArgsModel(BaseModel):
    class Config:
        extra = "forbid"

    path: str
    init_args: DictStrAny = Field(default_factory=dict)


T = TypeVar("T", SingleImage, InitArgsModel)


class InitTaskOutput(BaseModel):
    parallelization_list: list[InitArgsModel] = Field(default_factory=list)

    class Config:
        extra = "forbid"


def _run_non_parallel_task_init(
    *,
    task: Task,
    function_kwargs: DictStrAny,
    executor: ThreadPoolExecutor,
) -> InitTaskOutput:
    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_non_parallel(**input_kwargs)
        if task_output is None:
            task_output = InitTaskOutput()
        else:
            task_output = InitTaskOutput(**task_output)
        return task_output

    task_output = executor.submit(
        _wrapper_expand_kwargs, function_kwargs
    ).result()

    return InitTaskOutput(**task_output.dict())


def merge_outputs(
    task_outputs: list[TaskOutput],
) -> TaskOutput:

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


def _run_v1_task(
    *,
    filtered_images: list[SingleImage],
    task: TaskV1,
    converted_args: dict,
    executor,
) -> TaskOutput:
    list_function_kwargs = [
        dict(
            **converted_args,
        )
        for image in filtered_images
    ]
    if len(list_function_kwargs) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(list_function_kwargs)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.command(**input_kwargs)
        if task_output is None:
            task_output = TaskOutput()
        else:
            task_output = TaskOutput(**task_output)
        return task_output

    results = executor.map(_wrapper_expand_kwargs, list_function_kwargs)
    task_outputs = list(results)  # noqa

    return TaskOutput()


def _run_parallel_task(
    *,
    filtered_images: list[SingleImage],
    task: Task,
    wftask: WorkflowTask,
    executor,
) -> TaskOutput:

    list_function_kwargs = [
        dict(
            path=image.path,
            **wftask.args_parallel,
        )
        for image in filtered_images
    ]

    if len(list_function_kwargs) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(list_function_kwargs)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_parallel(**input_kwargs)
        if task_output is None:
            task_output = TaskOutput()
        else:
            task_output = TaskOutput(**task_output)
        return task_output

    results = executor.map(_wrapper_expand_kwargs, list_function_kwargs)
    task_outputs = list(results)

    merged_output = merge_outputs(
        task_outputs,
    )
    return merged_output


def deduplicate_list(
    this_list: list[T], PydanticModel: ModelMetaclass
) -> list[T]:
    """
    Custom replacement for `set(this_list)`, when items are Pydantic-model
    instances and then non-hashable (e.g. SingleImage or InitArgsModel).
    """
    this_list_dict = [this_item.dict() for this_item in this_list]
    new_list_dict = []
    for this_dict in this_list_dict:
        if this_dict not in new_list_dict:
            new_list_dict.append(this_dict)
    new_list = [PydanticModel(**this_dict) for this_dict in new_list_dict]
    return new_list


def _run_compound_task(
    *,
    filtered_images: list[SingleImage],
    zarr_dir: str,
    task: Task,
    wftask: WorkflowTask,
    executor,
) -> TaskOutput:
    # 3/A: non-parallel init task
    paths = [image.path for image in filtered_images]
    function_kwargs = dict(
        paths=paths,
        zarr_dir=zarr_dir,
        **wftask.args_non_parallel,
    )
    init_task_output = _run_non_parallel_task_init(
        task=task,
        function_kwargs=function_kwargs,
        executor=executor,
    )

    # 3/B: parallel part of a compound task
    parallelization_list = init_task_output.parallelization_list
    parallelization_list = deduplicate_list(
        parallelization_list, PydanticModel=InitArgsModel
    )
    list_function_kwargs = []
    for ind, parallelization_item in enumerate(parallelization_list):
        list_function_kwargs.append(
            dict(
                path=parallelization_item.path,
                init_args=parallelization_item.init_args,
                **wftask.args_parallel,
            )
        )

    if len(list_function_kwargs) > MAX_PARALLELIZATION_LIST_SIZE:
        raise ValueError(
            "Too many parallelization items.\n"
            f"   {len(list_function_kwargs)=}\n"
            f"   {MAX_PARALLELIZATION_LIST_SIZE=}\n"
        )

    def _wrapper_expand_kwargs(input_kwargs: dict[str, Any]):
        task_output = task.function_parallel(**input_kwargs)
        if task_output is None:
            task_output = TaskOutput()
        else:
            task_output = TaskOutput(**task_output)
        return task_output

    results = executor.map(_wrapper_expand_kwargs, list_function_kwargs)
    task_outputs = list(results)

    merged_output = merge_outputs(task_outputs)
    return merged_output
