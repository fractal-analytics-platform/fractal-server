from pathlib import Path
from typing import Optional
from typing import Union

from fractal_server.tasks.utils import slugify_task_name


def sanitize_component(value: str) -> str:
    """
    Remove {" ", "/", "."} form a string, e.g. going from
    'plate.zarr/B/03/0' to 'plate_zarr_B_03_0'.
    """
    return value.replace(" ", "_").replace("/", "_").replace(".", "_")


def task_subfolder_name(order: Union[str, int], task_name: str) -> str:
    task_name_slug = slugify_task_name(task_name)
    return f"{order}_{task_name_slug}"


class TaskFiles:
    """
    Group all file paths pertaining to a task

    Attributes:
        workflow_dir:
            Server-owned directory to store all task-execution-related relevant
            files (inputs, outputs, errors, and all meta files related to the
            job execution). Note: users cannot write directly to this folder.
        workflow_dir_user:
            User-side directory with the same scope as `workflow_dir`, and
            where a user can write.
        task_order:
            Positional order of the task within a workflow.
        component:
            Specific component to run the task for (relevant for tasks that
            will be executed in parallel over many components).
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

    workflow_dir: Path
    workflow_dir_user: Path
    subfolder: Path
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
        workflow_dir: Path,
        workflow_dir_user: Path,
        task_name: str,
        task_order: Optional[int] = None,
        component: Optional[str] = None,
    ):
        self.workflow_dir = workflow_dir
        self.workflow_dir_user = workflow_dir_user
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
            order = "task"
        self.file_prefix = f"{order}{component_safe}"
        self.subfolder_name = task_subfolder_name(
            order=order, task_name=self.task_name
        )
        self.subfolder = self.workflow_dir_user / self.subfolder_name
        self.args = self.subfolder / f"{self.file_prefix}.args.json"
        self.out = self.subfolder / f"{self.file_prefix}.out"
        self.err = self.subfolder / f"{self.file_prefix}.err"
        self.log = self.subfolder / f"{self.file_prefix}.log"
        self.metadiff = self.subfolder / f"{self.file_prefix}.metadiff.json"


def get_task_file_paths(
    workflow_dir: Path,
    workflow_dir_user: Path,
    task_name: str,
    task_order: Optional[int] = None,
    component: Optional[str] = None,
) -> TaskFiles:
    """
    Return the corrisponding TaskFiles object
    """
    return TaskFiles(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_name=task_name,
        task_order=task_order,
        component=component,
    )
