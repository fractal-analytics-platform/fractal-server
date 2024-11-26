from pathlib import Path

from fractal_server.app.runner.task_files import TaskFiles


def get_default_task_files(
    *, workflow_dir_local: Path, workflow_dir_remote: Path
) -> TaskFiles:
    """
    This will be called when self.submit or self.map are called from
    outside fractal-server, and then lack some optional arguments.
    """
    task_files = TaskFiles(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=None,
        task_name="name",
    )
    return task_files
