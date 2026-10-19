"""
Apply a resume plan: claim the job and update the database in one
transaction, and return what is needed to launch `submit_workflow`.
"""

from dataclasses import dataclass
from pathlib import Path

from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import Resource
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.utils import get_timestamp
from fractal_server.zip_tools import _zip_folder_to_file_and_remove

from .plan import ACCOUNTING_START_MARGIN
from .plan import BOOT_MARKER
from .plan import ResumeAction
from .plan import ResumeCase
from .plan import ResumePlan
from .spec import ResumeSpec

# Same as in `app/routes/api/v2/submit.py`
FRACTAL_CACHE_DIR = "fractal/.fractal_cache"
MARK_ONLY_REASONS = {
    ResumeCase.NO_WORKING_DIR: "The server stopped while the job was being "
    "submitted.",
    ResumeCase.ALL_TASKS_DONE: "All tasks had completed when the server "
    "stopped.",
    ResumeCase.TASK_FAILED: "A task had failed when the server stopped; see "
    "the task history for the error.",
}


class ResumeError(RuntimeError):
    pass


@dataclass
class ResumeLaunch:
    """
    Arguments of `submit_workflow`, except `fractal_ssh`.
    """

    workflow_id: int
    dataset_id: int
    job_id: int
    user_id: int
    worker_init: str | None
    user_cache_dir: str
    resource: Resource
    profile: Profile
    resume: ResumeSpec


def apply_resume_plan(plan: ResumePlan) -> ResumeLaunch | None:
    """
    Returns:
        What to launch, or `None` if the job was only marked done/failed.
    """
    if plan.action not in (
        ResumeAction.RESUME,
        ResumeAction.MARK_DONE,
        ResumeAction.MARK_FAILED,
    ):
        raise ResumeError(f"Nothing to apply for action '{plan.action}'.")

    now = get_timestamp()
    # Written to `job.log`: blocks a second resume by the same server boot,
    # and its `k=` lets a later plan recognise an interrupted resume
    marker = BOOT_MARKER
    with next(get_sync_db()) as db:
        job = db.execute(
            select(JobV2)
            .where(JobV2.id == plan.job_id)
            .with_for_update(skip_locked=True)
        ).scalar_one_or_none()
        if job is None:
            raise ResumeError("Job is being resumed by another request.")
        log = job.log or ""
        if job.status != JobStatusType.SUBMITTED:
            raise ResumeError(f"Job status is '{job.status}'.")
        if marker in log:
            raise ResumeError("Job was already resumed by this server.")
        # TODO(review): no limit on the number of resumes. If a task itself
        # kills the server process (e.g. OOM while the engine merges its
        # outputs into a large image list), its resume would crash the server
        # again at every restart. Discuss whether this is a real risk; if so,
        # mark the job failed after N resumes from the same task (`k=` in the
        # marker). Is this a real concern?

        info = plan.interrupted_task
        run_id = info.history_run_id if info is not None else None
        if run_id is not None and plan.action == ResumeAction.RESUME:
            # Units and their image-cache rows go by cascade; the rerun
            # creates new ones for the same images
            db.execute(delete(HistoryRun).where(HistoryRun.id == run_id))

        resumed_task = (
            f" k={plan.resume_from_task_index}"
            if plan.action == ResumeAction.RESUME
            else ""
        )
        job.log = (
            f"{log}\n{marker}{resumed_task} {plan.case} -> {plan.action} "
            f"at {now}\n"
        )
        if plan.action != ResumeAction.RESUME:
            status = (
                JobStatusType.DONE
                if plan.action == ResumeAction.MARK_DONE
                else JobStatusType.FAILED
            )
            # Same content as a normal job end (the workflow log), plus why
            # the job was closed without running
            working_dir = job.working_dir
            workflow_log = ""
            if working_dir is not None:
                log_path = Path(working_dir, WORKFLOW_LOG_FILENAME)
                if log_path.exists():
                    workflow_log = log_path.read_text()
            job.log = (
                f"{workflow_log}{job.log}"
                f"Job marked as {status} after a server restart. "
                f"{MARK_ONLY_REASONS.get(plan.case, '')}\n"
            )
            job.status = status
            job.end_timestamp = now
            db.commit()
        else:
            db.commit()

            user = (
                db.execute(
                    select(UserOAuth).where(UserOAuth.email == job.user_email)
                )
                .unique()
                .scalar_one()
            )
            project = db.get_one(ProjectV2, job.project_id)
            launch = ResumeLaunch(
                workflow_id=job.workflow_id,
                dataset_id=job.dataset_id,
                job_id=job.id,
                user_id=user.id,
                worker_init=job.worker_init,
                user_cache_dir=Path(
                    user.project_dirs[0], FRACTAL_CACHE_DIR
                ).as_posix(),
                resource=db.get_one(Resource, project.resource_id),
                profile=db.get_one(Profile, user.profile_id),
                resume=ResumeSpec(
                    first_task_index=plan.resume_from_task_index,
                    type_filters=plan.type_filters,
                    adopt_since=int(job.start_timestamp.timestamp())
                    - ACCOUNTING_START_MARGIN,
                ),
            )
            db.expunge_all()
            return launch

    # As at the end of `submit_workflow`
    if working_dir is not None and Path(working_dir).is_dir():
        _zip_folder_to_file_and_remove(folder=working_dir)
    return None
