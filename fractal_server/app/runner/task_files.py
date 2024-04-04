from pathlib import Path
from typing import Optional


def sanitize_component(value: str) -> str:
    """
    Remove {" ", "/", "."} form a string, e.g. going from
    'plate.zarr/B/03/0' to 'plate_zarr_B_03_0'.
    """
    return value.replace(" ", "_").replace("/", "_").replace(".", "_")


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
    task_order: Optional[int] = None
    component: Optional[str] = None  # FIXME: this is actually for V1 only

    file_prefix: str
    args: Path
    out: Path
    err: Path
    log: Path
    metadiff: Path

    def __init__(
        self,
        workflow_dir: Path,
        workflow_dir_user: Path,
        task_order: Optional[int] = None,
        component: Optional[str] = None,
    ):
        self.workflow_dir = workflow_dir
        self.workflow_dir_user = workflow_dir_user
        self.task_order = task_order
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
        self.args = self.workflow_dir_user / f"{self.file_prefix}.args.json"
        self.out = self.workflow_dir_user / f"{self.file_prefix}.out"
        self.err = self.workflow_dir_user / f"{self.file_prefix}.err"
        self.log = self.workflow_dir_user / f"{self.file_prefix}.log"
        self.metadiff = (
            self.workflow_dir_user / f"{self.file_prefix}.metadiff.json"
        )


def get_task_file_paths(
    workflow_dir: Path,
    workflow_dir_user: Path,
    task_order: Optional[int] = None,
    component: Optional[str] = None,
) -> TaskFiles:
    """
    Return the corrisponding TaskFiles object

    This function is mainly used as a cache to avoid instantiating needless
    objects.
    """
    return TaskFiles(
        workflow_dir=workflow_dir,
        workflow_dir_user=workflow_dir_user,
        task_order=task_order,
        component=component,
    )
