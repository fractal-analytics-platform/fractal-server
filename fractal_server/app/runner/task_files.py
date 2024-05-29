from pathlib import Path
from typing import Optional
from typing import Union

from fractal_server.tasks.utils import slugify_task_name


def sanitize_component(value: str) -> str:
    """
    Remove {" ", "/", "."} form a string, e.g. going from
    'plate.zarr/B/03/0' to 'plate_zarr_B_03_0'.

    Args:
        value: Input strig
    """
    return value.replace(" ", "_").replace("/", "_").replace(".", "_")


def task_subfolder_name(order: Union[int, str], task_name: str) -> str:
    """
    Get name of task-specific subfolder.

    Args:
        order:
        task_name:
    """
    task_name_slug = slugify_task_name(task_name)
    return f"{order}_{task_name_slug}"


class TaskFiles:
    """
    Group all file paths pertaining to a task

    Attributes:
        workflow_dir_local:
            Server-owned directory to store all task-execution-related relevant
            files. Note: users cannot write directly to this folder.
        workflow_dir_remote:
            User-side directory with the same scope as `workflow_dir_local`,
            and where a user can write.
        subfolder_name:
            Name of task-specific subfolder
        remote_subfolder:
            Path to user-side task-specific subfolder
        task_name:
            Name of the task
        task_order:
            Positional order of the task within a workflow.
        component:
            Specific component to run the task for (relevant for tasks to be
            executed in parallel over many components).
        file_prefix:
            Prefix for all task-related files.
        args:
            Path for input json file.
        metadiff:
            Path for output json file with metadata update.
        out:
            Path for task-execution stdout.
        err:
            Path for task-execution stderr.
    """

    workflow_dir_local: Path
    workflow_dir_remote: Path
    remote_subfolder: Path
    subfolder_name: str
    task_name: str
    task_order: Optional[int] = None
    component: Optional[str] = None

    file_prefix: str
    file_prefix_with_subfolder: str
    args: Path
    out: Path
    err: Path
    log: Path
    metadiff: Path

    def __init__(
        self,
        workflow_dir_local: Path,
        workflow_dir_remote: Path,
        task_name: str,
        task_order: Optional[int] = None,
        component: Optional[str] = None,
    ):
        self.workflow_dir_local = workflow_dir_local
        self.workflow_dir_remote = workflow_dir_remote
        self.task_order = task_order
        self.task_name = task_name
        self.component = component

        if self.component is not None:
            component_safe = sanitize_component(str(self.component))
            component_safe = f"_par_{component_safe}"
        else:
            component_safe = ""

        if self.task_order is not None:
            order = str(self.task_order)
        else:
            order = "0"
        self.file_prefix = f"{order}{component_safe}"
        self.subfolder_name = task_subfolder_name(
            order=order, task_name=self.task_name
        )
        self.remote_subfolder = self.workflow_dir_remote / self.subfolder_name
        self.args = self.remote_subfolder / f"{self.file_prefix}.args.json"
        self.out = self.remote_subfolder / f"{self.file_prefix}.out"
        self.err = self.remote_subfolder / f"{self.file_prefix}.err"
        self.log = self.remote_subfolder / f"{self.file_prefix}.log"
        self.metadiff = (
            self.remote_subfolder / f"{self.file_prefix}.metadiff.json"
        )


def get_task_file_paths(
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    task_name: str,
    task_order: Optional[int] = None,
    component: Optional[str] = None,
) -> TaskFiles:
    """
    Return the corrisponding TaskFiles object
    """
    return TaskFiles(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_name=task_name,
        task_order=task_order,
        component=component,
    )
