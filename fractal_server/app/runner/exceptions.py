class TaskExecutionError(RuntimeError):
    """
    Forwards errors occurred during the execution of a task

    This error wraps and forwards errors occurred during the execution of
    tasks, when the exit code is larger than 0 (i.e. the error took place
    within the task). This error also adds information that is useful to track
    down and debug the failing task within a workflow.

    Attributes:
        workflow_task_id:
            ID of the workflow task that failed.
        workflow_task_order:
            Order of the task within the workflow.
        task_name:
            Human readable name of the failing task.
    """

    workflow_task_id: int | None = None
    workflow_task_order: int | None = None
    task_name: str | None = None

    def __init__(
        self,
        *args,
        workflow_task_id: int | None = None,
        workflow_task_order: int | None = None,
        task_name: str | None = None,
    ):
        super().__init__(*args)
        self.workflow_task_id = workflow_task_id
        self.workflow_task_order = workflow_task_order
        self.task_name = task_name


class TaskOutputValidationError(ValueError):
    pass


class JobExecutionError(RuntimeError):
    """
    JobExecutionError

    Attributes:
        info:
            A free field for additional information
    """

    info: str | None = None

    def __init__(
        self,
        *args,
        info: str | None = None,
    ):
        super().__init__(*args)
        self.info = info

    def assemble_error(self) -> str:
        if self.info:
            content = f"\n{self.info}\n\n"
        else:
            content = str(self)
        message = f"JobExecutionError\n{content}"
        return message
