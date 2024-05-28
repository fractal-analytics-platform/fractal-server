from pathlib import Path

from fractal_server.app.runner.task_files import task_subfolder_name
from tests.fixtures_tasks_v1 import MockWorkflowTask


def _create_task_subfolder(
    *, wftask: MockWorkflowTask, workflow_dir_local: Path
) -> Path:
    """
    This is an operation that takes place in higher-level Fractal-runner
    functions, and thus must be called explicitly here.
    """
    subfolder = workflow_dir_local / task_subfolder_name(
        order=wftask.order, task_name=wftask.task.name
    )
    subfolder.mkdir()
    return subfolder
