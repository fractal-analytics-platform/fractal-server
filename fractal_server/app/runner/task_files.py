from pathlib import Path
from typing import Optional
from typing import Union

from pydantic import BaseModel

from fractal_server.string_tools import sanitize_string


def task_subfolder_name(order: Union[int, str], task_name: str) -> str:
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
    Group all file paths pertaining to a task     FIXME
    """

    # Parent directory
    root_dir_local: Path
    root_dir_remote: Path

    # Per-wftask
    task_name: str
    task_order: int

    # Per-single-component
    component: Optional[str] = None

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
    def log_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local / f"{self.component}-log.txt"
        ).as_posix()

    @property
    def log_file_remote(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.component}-log.txt"
        ).as_posix()

    @property
    def args_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local / f"{self.component}-args.json"
        ).as_posix()

    @property
    def args_file_remote(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.component}-args.json"
        ).as_posix()

    @property
    def metadiff_file_local(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_local / f"{self.component}-metadiff.json"
        ).as_posix()

    @property
    def metadiff_file_remote(self) -> str:
        self._check_component()
        return (
            self.wftask_subfolder_remote / f"{self.component}-metadiff.json"
        ).as_posix()
