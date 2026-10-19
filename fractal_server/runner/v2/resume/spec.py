from dataclasses import dataclass


@dataclass(frozen=True)
class ResumeSpec:
    """
    How `submit_workflow` continues an orphaned job.

    Attributes:
        first_task_index: Task to restart from (overrides the job value).
        type_filters: Type filters replayed up to that task.
        adopt_since: Unix timestamp, lower bound for adoptable SLURM jobs.
    """

    first_task_index: int
    type_filters: dict[str, bool]
    adopt_since: int
