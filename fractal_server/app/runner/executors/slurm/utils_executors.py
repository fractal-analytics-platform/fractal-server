from ...task_files import TaskFiles


def get_default_task_files(self) -> TaskFiles:
    """
    This will be called when self.submit or self.map are called from
    outside fractal-server, and then lack some optional arguments.
    """
    task_files = TaskFiles(
        workflow_dir_local=self.workflow_dir_local,
        workflow_dir_remote=self.workflow_dir_remote,
        task_order=None,
        task_name="name",
    )
    return task_files
