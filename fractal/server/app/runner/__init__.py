import importlib
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import parsl
from parsl.app.python import PythonApp
from parsl.config import Config

from ..models.project import Dataset
from ..models.project import Resource
from ..models.task import Subtask
from ..models.task import Task


parsl.load(Config())


def _atomic_task_factory(
    *,
    task: Union[Task, Subtask],
    input_paths: List[Path],
    output_path: Path,
    metadata: Optional[Dict[str, Any]],
) -> PythonApp:
    """
    Single task processing

    Create a single PARSL app that encapsulates the task at hand and
    its parallelizazion.
    """

    if isinstance(task, Task):
        task_args = task.default_args
    elif isinstance(task, Subtask):
        task_args = task._merged_args
    else:
        raise ValueError(
            "Argument `task` must be of type `Task` or `Subtask`. "
            f"Got `{type(task)}`"
        )

    @parsl.python_app()
    def _task_app(
        input_paths: List[Path] = input_paths,
        output_path: Path = output_path,
        metadata: Optional[Dict[str, Any]] = metadata,
        task_args: Optional[Dict[str, Any]] = task_args,
    ):
        task_module = importlib.import_module(task.import_path)
        _callable = getattr(task_module, task.callable)
        _callable(
            input_paths=input_paths,
            output_path=output_path,
            metadata=metadata,
            **task_args,
        )

    parall_level = task_args.get("parallelization_level", None)
    if parall_level:

        @parsl.join_app()
        def _task_parallelization(parall_level):
            # TODO
            # Write generator that takes care of generating
            # parallelization_items
            #
            # This is just a placeholder implementation, likely nonsensical
            parall_item_gen = (par_item for par_item in metadata[parall_level])
            map(lambda item: _task_app(component=item), parall_item_gen)

        return _task_parallelization()
    else:
        return _task_app()


async def submit_workflow(
    *,
    input_dataset: Dataset,
    workflow: Task,
    output_dataset: Optional[Union[Dataset, str]] = None,
    output_resource: Optional[Union[Resource, str]] = None,
):
    """
    Arguments
    ---------
    output_dataset (Dataset | str) :
        the destination dataset of the workflow. If not provided, overwriting
        of the input dataset is implied and an error is raised if the dataset
        is in read only mode. If a string is passed and the dataset does not
        exist, a new dataset with that name is created and within it a new
        resource with the same name.
    """
    if output_dataset is None:
        if input_dataset.read_only:
            raise ValueError(
                "Input dataset is read-only: cannot overwrite. "
                "Please provide `output_dataset` explicitly."
            )
        else:
            output_dataset = input_dataset
    else:
        pass
        # TODO: check that dataset exists and if not create dataset and
        # resource.

    if workflow.input_type != input_dataset.type:
        raise TypeError(
            f"Incompatible types `{workflow.input_type}` of workflow "
            f"`{workflow.name}` and `{input_dataset.type}` of dataset "
            f"`{input_dataset.name}`"
        )

    input_path = [r.glob_path for r in input_dataset.resource_list]
    output_path = None
    metadata = input_dataset.metadata

    if "workflow" in workflow.resource_type:
        for subtask in workflow.subtask_list:
            kwargs = subtask._merged_args

            @parsl.python_app()
            def workflow_app(
                input_path: List[Path] = input_path,
                output_path: Path = output_path,
                metadata: Dict[str, Any] = metadata,
                kwargs: Dict[str, Any] = kwargs,
            ):
                task_module = importlib.import_module(subtask.import_path)
                __callable = getattr(task_module, subtask.callable)
                __callable(
                    input_path=input_path,
                    output_path=output_dataset,
                    metadata=metadata,
                    **kwargs,
                )

    app_list = []
    # TODO
    # for parallelization_item in parallelization_level:
    #     app_list.append(workflow_app(parallelization_item))

    map(lambda app: app.result(), app_list)
