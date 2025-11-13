from pydantic import BaseModel
from pydantic import ConfigDict


class JobRunnerConfigLocal(BaseModel):
    """
    Runner-configuration specifications, for a `local` resource.

    The typical use case is that setting `parallel_tasks_per_job` to a
    small number (e.g. 1) will limit parallelism when executing tasks
    requiring a large amount of resources (e.g. memory) on a local machine.

    Attributes:
        parallel_tasks_per_job:
            Maximum number of tasks to be run in parallel within a local
            runner. If `None`, then all tasks may start at the same time.
    """

    model_config = ConfigDict(extra="forbid")
    parallel_tasks_per_job: int | None = None

    @property
    def batch_size(self) -> int:
        return self.parallel_tasks_per_job or 0
