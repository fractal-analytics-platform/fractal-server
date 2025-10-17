from pydantic import BaseModel
from pydantic import ConfigDict


class JobRunnerConfigLocal(BaseModel):
    """
    Specifications of the local-backend configuration

    Attributes:
        parallel_tasks_per_job:
            Maximum number of tasks to be run in parallel as part of a call to
            `FractalThreadPoolExecutor.map`; if `None`, then all tasks will
            start at the same time.
    """

    model_config = ConfigDict(extra="forbid")
    parallel_tasks_per_job: int | None = None

    @property
    def batch_size(self) -> int:
        return self.parallel_tasks_per_job or 1
