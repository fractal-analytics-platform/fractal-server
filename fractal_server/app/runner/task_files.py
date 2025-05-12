from pathlib import Path

from pydantic import BaseModel

from fractal_server.app.runner.components import _index_to_component
from fractal_server.string_tools import sanitize_string

SUBMIT_PREFIX = "non_par"
MULTISUBMIT_PREFIX = "par"


def task_subfolder_name(
    order: int | str,
    task_name: str,
) -> str:
    """
    Get name of task-specific subfolder.

    Args:
        order:
        task_name:
    """
    task_name_slug = sanitize_string(task_name)
    return f"{order}_{task_name_slug}"


class TaskFiles(BaseModel):
    """
    Files related to a task.

    Attributes:
        root_dir_local:
        root_dir_remote:
        task_name:
        task_order:
        component:
        prefix:
    """

    # Parent directory
    root_dir_local: Path
    root_dir_remote: Path

    # Per-wftask
    task_name: str
    task_order: int

    # Per-single-component
    component: str | None = None
    prefix: str | None = None

    def _check_component(self):
        if self.component is None:
            raise ValueError("`component` cannot be None")

    @property
    def subfolder_name(self) -> str:
        order = str(self.task_order or 0)
        return task_subfolder_name(
            order=order,
            task_name=self.task_name,
        )

    @property
    def wftask_subfolder_remote(self) -> Path:
        return self.root_dir_remote / self.subfolder_name

    @property
    def wftask_subfolder_local(self) -> Path:
        return self.root_dir_local / self.subfolder_name

    @property
    def prefix_component(self):
        if self.prefix is None:
            return self.component
        else:
            return f"{self.prefix}-{self.component}"

    @property
    def log_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local / f"{self.prefix_component}-log.txt"
        ).as_posix()

    @property
    def log_file_remote_path(self) -> Path:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.prefix_component}-log.txt"
        )

    @property
    def log_file_remote(self) -> str:
        return self.log_file_remote_path.as_posix()

    @property
    def args_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local / f"{self.prefix_component}-args.json"
        ).as_posix()

    @property
    def args_file_remote_path(self) -> Path:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.prefix_component}-args.json"
        )

    @property
    def args_file_remote(self) -> str:
        return self.args_file_remote_path.as_posix()

    @property
    def metadiff_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local
            / f"{self.prefix_component}-metadiff.json"
        ).as_posix()

    @property
    def metadiff_file_remote_path(self) -> Path:
        self._check_component()
        return (
            self.wftask_subfolder_remote
            / f"{self.prefix_component}-metadiff.json"
        )

    @property
    def metadiff_file_remote(self) -> str:
        return self.metadiff_file_remote_path.as_posix()


def enrich_task_files_multisubmit(
    *,
    tot_tasks: int,
    batch_size: int,
    base_task_files: TaskFiles,
) -> list[TaskFiles]:
    """
    Expand `TaskFiles` objects with `component` and `prefix`.
    """

    new_list_task_files: list[TaskFiles] = []
    for absolute_index in range(tot_tasks):
        ind_batch = absolute_index // batch_size
        new_list_task_files.append(
            TaskFiles(
                **base_task_files.model_dump(
                    exclude={
                        "component",
                        "prefix",
                    }
                ),
                prefix=f"{MULTISUBMIT_PREFIX}-{ind_batch:06d}",
                component=_index_to_component(absolute_index),
            )
        )
    return new_list_task_files
