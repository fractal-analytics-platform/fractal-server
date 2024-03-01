import os
from typing import Optional


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

    workflow_task_id: Optional[int] = None
    workflow_task_order: Optional[int] = None
    task_name: Optional[str] = None

    def __init__(
        self,
        *args,
        workflow_task_id: Optional[int] = None,
        workflow_task_order: Optional[int] = None,
        task_name: Optional[str] = None,
    ):
        super().__init__(*args)
        self.workflow_task_id = workflow_task_id
        self.workflow_task_order = workflow_task_order
        self.task_name = task_name


class JobExecutionError(RuntimeError):
    """
    Forwards errors in the execution of a task that are due to external factors

    This error wraps and forwards errors occurred during the execution of
    tasks, but related to external factors like:

    1. A negative exit code (e.g. because the task received a TERM or KILL
       signal);
    2. An error on the executor side (e.g. the SLURM executor could not
       find the pickled file with task output).

    This error also adds information that is useful to track down and debug the
    failing task within a workflow.

    Attributes:
        info:
            A free field for additional information
        cmd_file:
            Path to the file of the command that was executed (e.g. a SLURM
            submission script).
        stdout_file:
            Path to the file with the command stdout
        stderr_file:
            Path to the file with the command stderr
    """

    cmd_file: Optional[str] = None
    stdout_file: Optional[str] = None
    stderr_file: Optional[str] = None
    info: Optional[str] = None

    def __init__(
        self,
        *args,
        cmd_file: Optional[str] = None,
        stdout_file: Optional[str] = None,
        stderr_file: Optional[str] = None,
        info: Optional[str] = None,
    ):
        super().__init__(*args)
        self.cmd_file = cmd_file
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.info = info

    def _read_file(self, filepath: str) -> str:
        """
        Return the content of a text file, and handle the cases where it is
        empty or missing
        """
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                content = f.read()
                if content:
                    return f"Content of {filepath}:\n{content}"
                else:
                    return f"File {filepath} is empty\n"
        else:
            return f"File {filepath} is missing\n"

    def assemble_error(self) -> str:
        """
        Read the files that are specified in attributes, and combine them in an
        error message.
        """
        if self.cmd_file:
            content = self._read_file(self.cmd_file)
            cmd_content = f"COMMAND:\n{content}\n\n"
        else:
            cmd_content = ""
        if self.stdout_file:
            content = self._read_file(self.stdout_file)
            out_content = f"STDOUT:\n{content}\n\n"
        else:
            out_content = ""
        if self.stderr_file:
            content = self._read_file(self.stderr_file)
            err_content = f"STDERR:\n{content}\n\n"
        else:
            err_content = ""

        content = f"{cmd_content}{out_content}{err_content}"
        if self.info:
            content = f"{content}ADDITIONAL INFO:\n{self.info}\n\n"

        if not content:
            content = str(self)
        message = f"JobExecutionError\n\n{content}"
        return message
