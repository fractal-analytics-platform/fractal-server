from pathlib import Path
from typing import Optional
from typing import Union

from pydantic import BaseModel

from fractal_server.string_tools import sanitize_string

SUBMIT_PREFIX = "non_par"
MULTISUBMIT_PREFIX = "par"


def task_subfolder_name(
    order: Union[int, str],
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
    component: Optional[str] = None
    prefix: Optional[str] = None

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
    def log_file_remote(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.prefix_component}-log.txt"
        ).as_posix()

    @property
    def args_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local / f"{self.prefix_component}-args.json"
        ).as_posix()

    @property
    def args_file_remote(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.prefix_component}-args.json"
        ).as_posix()

    @property
    def metadiff_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local
            / f"{self.prefix_component}-metadiff.json"
        ).as_posix()

    @property
    def metadiff_file_remote(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_remote
            / f"{self.prefix_component}-metadiff.json"
        ).as_posix()

    @property
    def remote_files_dict(self) -> dict[str, str]:
        return dict(
            args_file_remote=self.args_file_remote,
            metadiff_file_remote=self.metadiff_file_remote,
            log_file_remote=self.log_file_remote,
        )
