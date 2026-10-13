"""
Read-only planning of the resume of orphaned jobs.

A job is an orphan if it is `submitted`, runs on a SLURM REST resource and
started before the cutoff (by default, the start of this server process).
The database tells where the job stopped; SLURM (via the stdout path stored
for each job) and the
remote output files tell what happened to the interrupted task.
"""

import re
from copy import copy
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import ValidationError
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import TaskType
from fractal_server.images.status_tools import IMAGE_STATUS_KEY
from fractal_server.images.tools import filter_image_list
from fractal_server.images.tools import merge_type_filters
from fractal_server.logger import set_logger
from fractal_server.runner.executors.slurm_rest.client import SlurmRestClient
from fractal_server.runner.executors.slurm_rest.settings import (
    get_slurm_rest_settings,
)
from fractal_server.runner.executors.slurm_rest.settings import (
    is_slurm_rest_enabled,
)
from fractal_server.runner.executors.slurm_rest.token import SlurmTokenProvider
from fractal_server.runner.task_files import MULTISUBMIT_PREFIX
from fractal_server.runner.task_files import SUBMIT_PREFIX
from fractal_server.runner.task_files import task_subfolder_name
from fractal_server.runner.v2.merge_outputs import merge_outputs
from fractal_server.runner.v2.task_interface import TaskOutput
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.utils import get_timestamp

from .slurm_evidence import FractalSlurmJob
from .slurm_evidence import collect_fractal_slurm_jobs

logger = set_logger(__name__)

RESUME_MARKER = "RESUME boot="
# Marker line of a resume that relaunched the job, see `apply_resume_plan`
_RESUMED_TASK_PATTERN = re.compile(
    rf"^{re.escape(RESUME_MARKER)}\S+ k=(\d+) ", flags=re.MULTILINE
)
# Imported at app creation, so this is (roughly) the server start time
SERVER_BOOT_TIMESTAMP = get_timestamp()
# Margin between server and SLURM clocks, for the accounting query
ACCOUNTING_START_MARGIN = 3600
WFTASK_DUMP_KEYS = (
    "task_id",
    "order",
    "type_filters",
    "args_parallel",
    "args_non_parallel",
    "meta_parallel",
    "meta_non_parallel",
)
CONVERTER_TYPES = (
    TaskType.CONVERTER_NON_PARALLEL,
    TaskType.CONVERTER_COMPOUND,
)


class ResumeCase(StrEnum):
    NO_WORKING_DIR = "C0_no_working_dir"
    NO_HISTORY = "C1_no_history"
    ALL_TASKS_DONE = "C2_all_tasks_done"
    TASK_FAILED = "C3_task_failed"
    NEXT_TASK_NOT_STARTED = "C4_next_task_not_started"
    RUN_WITHOUT_UNITS = "C5_run_without_units"
    UNITS_IN_PROGRESS = "C6_units_in_progress"
    UNITS_FINISHED = "C7_units_finished_run_open"


class ResumeAction(StrEnum):
    NONE = "none"
    RESUME = "resume"
    MARK_DONE = "mark_done"
    MARK_FAILED = "mark_failed"
    REFUSE = "refuse"


class InterruptedTask(BaseModel):
    task_index: int
    workflowtask_id: int
    task_name: str
    task_type: str
    subfolder: str
    history_run_id: int | None = None
    unit_status_counts: dict[str, int] = Field(default_factory=dict)
    num_selected_images: int | None = None
    selection_matches_units: bool | None = None
    outputs_reflected_in_dataset: bool | None = None
    slurm_jobs: list[FractalSlurmJob] = Field(default_factory=list)


class ResumePlan(BaseModel):
    job_id: int
    job_status: str
    start_timestamp: datetime
    resumable: bool = False
    case: ResumeCase | None = None
    action: ResumeAction = ResumeAction.NONE
    resume_from_task_index: int | None = None
    type_filters: dict[str, bool] | None = None
    interrupted_task: InterruptedTask | None = None
    other_slurm_jobs: list[FractalSlurmJob] = Field(default_factory=list)
    resume_marker_present: bool = False
    previously_resumed_task_index: int | None = None
    reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


@dataclass
class _RemoteContext:
    job_id: int
    start_timestamp: datetime
    remote_root: str
    last_task_index: int
    resource: Resource
    profile: Profile
    task_type: str
    output_types: dict[str, bool]
    later_subfolders: set[str]
    dataset_images: list[dict[str, Any]]
    unit_selection: list[str] | None
    predicted_selection: list[str] | None
    any_unit_failed: bool


def _refuse(plan: ResumePlan, reason: str) -> None:
    plan.action = ResumeAction.REFUSE
    plan.reasons.append(reason)
    return None


def _subfolder(wftask: WorkflowTaskV2) -> str:
    # Same as `TaskFiles.subfolder_name`
    return task_subfolder_name(
        order=str(wftask.order or 0), task_name=wftask.task.name
    )


def _changed_dump_keys(
    dump: dict[str, Any], wftask: WorkflowTaskV2
) -> list[str]:
    return [
        key for key in WFTASK_DUMP_KEYS if dump.get(key) != getattr(wftask, key)
    ]


def _selection_from_units(
    task_type: str, units: list[HistoryUnit]
) -> list[str] | None:
    """
    Image selection at submission time, as stored in the history units.
    """
    if task_type in CONVERTER_TYPES:
        return None
    if task_type == TaskType.PARALLEL:
        return [url for unit in units for url in unit.zarr_urls]
    # Non-parallel unit, or compound init unit (the first one)
    return list(units[0].zarr_urls) if units else []


def last_resumed_task_index(log: str | None) -> int | None:
    """
    Task index from which the latest resume relaunched the job, if any.
    """
    matches = _RESUMED_TASK_PATTERN.findall(log or "")
    return int(matches[-1]) if matches else None


def list_orphan_candidates(*, started_before: datetime) -> list[int]:
    with next(get_sync_db()) as db:
        stm = (
            select(JobV2.id)
            .where(JobV2.status == JobStatusType.SUBMITTED)
            .where(JobV2.start_timestamp < started_before)
            .order_by(JobV2.id)
        )
        return list(db.execute(stm).scalars().all())


def plan_resume(
    *,
    job_id: int,
    fractal_ssh_list: FractalSSHList | None,
    started_before: datetime | None = None,
    check_remote: bool = True,
) -> ResumePlan | None:
    """
    Build the resume plan of a job, without changing anything.

    Args:
        job_id:
        fractal_ssh_list: Used for `scontrol token` and remote outputs.
        started_before: If set, jobs started later are not orphans.
        check_remote: Whether to query SLURM and read remote files.

    Returns:
        The plan, or `None` if the job does not exist.
    """
    with next(get_sync_db()) as db:
        job = db.get(JobV2, job_id)
        if job is None:
            return None
        plan = ResumePlan(
            job_id=job.id,
            job_status=job.status,
            start_timestamp=job.start_timestamp,
            resume_marker_present=RESUME_MARKER in (job.log or ""),
            previously_resumed_task_index=last_resumed_task_index(job.log),
        )
        ctx = _plan_from_db(
            db=db, job=job, plan=plan, started_before=started_before
        )

    if ctx is None:
        return plan
    if not check_remote:
        plan.warnings.append("SLURM and remote-file checks were skipped.")
        return plan
    try:
        _add_remote_evidence(
            ctx=ctx, plan=plan, fractal_ssh_list=fractal_ssh_list
        )
    except Exception as e:
        logger.error(f"Remote checks for job {job_id} failed: {e}")
        _refuse(plan, f"SLURM or remote-file checks failed: {e}")
    return plan


def _plan_from_db(
    *,
    db,
    job: JobV2,
    plan: ResumePlan,
    started_before: datetime | None,
) -> _RemoteContext | None:
    if job.status != JobStatusType.SUBMITTED:
        plan.reasons.append(f"Job status is '{job.status}'.")
        return None
    if started_before is not None and job.start_timestamp >= started_before:
        plan.reasons.append(
            f"Job started after {started_before.isoformat()}, "
            "so a live server process owns it."
        )
        return None
    if None in (job.project_id, job.workflow_id, job.dataset_id):
        return _refuse(plan, "Project, workflow or dataset was deleted.")

    project = db.get_one(ProjectV2, job.project_id)
    workflow = db.get_one(WorkflowV2, job.workflow_id)
    dataset = db.get_one(DatasetV2, job.dataset_id)
    resource = db.get_one(Resource, project.resource_id)
    if resource.type != ResourceType.SLURM_SSH or not is_slurm_rest_enabled():
        plan.reasons.append(
            f"Resource '{resource.name}' does not use the SLURM REST runner."
        )
        return None
    plan.resumable = True

    profile_id = db.execute(
        select(UserOAuth.profile_id).where(UserOAuth.email == job.user_email)
    ).scalar_one_or_none()
    if profile_id is None:
        return _refuse(plan, f"No user or profile for '{job.user_email}'.")
    profile = db.get_one(Profile, profile_id)
    if profile.resource_id != resource.id:
        return _refuse(plan, "User profile is not on the project resource.")

    if job.working_dir is None or job.working_dir_user is None:
        plan.case = ResumeCase.NO_WORKING_DIR
        plan.action = ResumeAction.MARK_FAILED
        return None

    first, last = job.first_task_index, job.last_task_index
    wftasks = workflow.task_list
    if last >= len(wftasks):
        return _refuse(plan, "Workflow has fewer tasks than at submission.")
    index_by_wftask_id = {
        wftasks[idx].id: idx for idx in range(first, last + 1)
    }

    runs = (
        db.execute(
            select(HistoryRun)
            .where(HistoryRun.job_id == job.id)
            .order_by(HistoryRun.id)
        )
        .scalars()
        .all()
    )
    latest_runs: dict[int, HistoryRun] = {}
    for run in runs:
        if run.workflowtask_id not in index_by_wftask_id:
            return _refuse(
                plan,
                f"HistoryRun {run.id} does not match the current tasks "
                "of the workflow.",
            )
        latest_runs[run.workflowtask_id] = run

    if any(
        run.status == HistoryUnitStatus.FAILED for run in latest_runs.values()
    ):
        plan.case = ResumeCase.TASK_FAILED
        plan.action = ResumeAction.MARK_FAILED
        return None

    # Replay the type filters that the engine accumulates across tasks
    type_filters = copy(job.type_filters)
    k = None
    for idx in range(first, last + 1):
        run = latest_runs.get(wftasks[idx].id)
        if run is None or run.status != HistoryUnitStatus.DONE:
            k = idx
            break
        type_filters.update(wftasks[idx].task.output_types)
    if k is None:
        plan.case = ResumeCase.ALL_TASKS_DONE
        plan.action = ResumeAction.MARK_DONE
        return None
    if any(index_by_wftask_id[wid] > k for wid in latest_runs):
        return _refuse(plan, f"Found runs for tasks after task {k}.")

    wftask = wftasks[k]
    run = latest_runs.get(wftask.id)
    plan.type_filters = type_filters
    plan.resume_from_task_index = k
    plan.action = ResumeAction.RESUME
    info = InterruptedTask(
        task_index=k,
        workflowtask_id=wftask.id,
        task_name=wftask.task.name,
        task_type=wftask.task_type,
        subfolder=_subfolder(wftask),
    )
    plan.interrupted_task = info

    units: list[HistoryUnit] = []
    if run is None:
        plan.case = (
            ResumeCase.NO_HISTORY
            if not latest_runs
            else ResumeCase.NEXT_TASK_NOT_STARTED
        )
    else:
        info.history_run_id = run.id
        changed = _changed_dump_keys(run.workflowtask_dump, wftask)
        if changed:
            return _refuse(
                plan, f"Task {k} changed since it started: {changed}."
            )
        units = (
            db.execute(
                select(HistoryUnit)
                .where(HistoryUnit.history_run_id == run.id)
                .order_by(HistoryUnit.id)
            )
            .scalars()
            .all()
        )
        for unit in units:
            info.unit_status_counts[unit.status] = (
                info.unit_status_counts.get(unit.status, 0) + 1
            )
        if not units:
            plan.case = ResumeCase.RUN_WITHOUT_UNITS
        elif any(u.status == HistoryUnitStatus.SUBMITTED for u in units):
            plan.case = ResumeCase.UNITS_IN_PROGRESS
        else:
            plan.case = ResumeCase.UNITS_FINISHED

    uses_status_filter = IMAGE_STATUS_KEY in job.attribute_filters
    if uses_status_filter and (k != first or units):
        return _refuse(
            plan,
            "The image-status filter would select different images on resume.",
        )

    # Image selection that the engine would compute on resume
    predicted_selection = None
    if wftask.task_type not in CONVERTER_TYPES and not uses_status_filter:
        task_type_filters = copy(type_filters)
        task_type_filters.update(
            merge_type_filters(
                task_input_types=wftask.task.input_types,
                wftask_type_filters=wftask.type_filters,
            )
        )
        selected = filter_image_list(
            filter_image_list(dataset.images, type_filters=task_type_filters),
            attribute_filters=job.attribute_filters,
        )
        predicted_selection = [img["zarr_url"] for img in selected]
        info.num_selected_images = len(predicted_selection)

    unit_selection = _selection_from_units(wftask.task_type, units)
    if units and predicted_selection is not None:
        info.selection_matches_units = predicted_selection == unit_selection
        # For C7 a mismatch is expected if results were already saved
        if (
            plan.case == ResumeCase.UNITS_IN_PROGRESS
            and not info.selection_matches_units
        ):
            return _refuse(
                plan, "Images selected now differ from the running units."
            )

    return _RemoteContext(
        job_id=job.id,
        start_timestamp=job.start_timestamp,
        remote_root=job.working_dir_user,
        last_task_index=last,
        resource=resource,
        profile=profile,
        task_type=wftask.task_type,
        output_types=dict(wftask.task.output_types),
        later_subfolders={
            _subfolder(wftasks[idx]) for idx in range(k + 1, last + 1)
        },
        dataset_images=dataset.images,
        unit_selection=unit_selection,
        predicted_selection=predicted_selection,
        any_unit_failed=any(
            u.status == HistoryUnitStatus.FAILED for u in units
        ),
    )


def _add_remote_evidence(
    *,
    ctx: _RemoteContext,
    plan: ResumePlan,
    fractal_ssh_list: FractalSSHList | None,
) -> None:
    if fractal_ssh_list is None:
        raise RuntimeError("No SSH connection list available.")
    fractal_ssh = fractal_ssh_list.get(
        host=ctx.resource.host,
        user=ctx.profile.username,
        key_path=ctx.profile.ssh_key_path,
    )
    fractal_ssh.check_connection()
    settings = get_slurm_rest_settings()
    token_provider = SlurmTokenProvider(
        fractal_ssh=fractal_ssh,
        lifespan=settings.FRACTAL_SLURM_REST_TOKEN_LIFESPAN,
    )
    client = SlurmRestClient(
        base_url=settings.FRACTAL_SLURM_REST_URL,
        api_version=settings.FRACTAL_SLURM_REST_API_VERSION,
        username=ctx.profile.username,
        get_token=token_provider.get_token,
        verify_tls=settings.FRACTAL_SLURM_REST_VERIFY_TLS,
        ca_bundle=settings.FRACTAL_SLURM_REST_CA_BUNDLE,
        timeout=settings.FRACTAL_SLURM_REST_TIMEOUT,
    )
    found_jobs = collect_fractal_slurm_jobs(
        client=client,
        job_root=ctx.remote_root,
        start_time=int(ctx.start_timestamp.timestamp())
        - ACCOUNTING_START_MARGIN,
    )

    info = plan.interrupted_task
    for job in found_jobs:
        if job.subfolder == info.subfolder:
            info.slurm_jobs.append(job)
        else:
            plan.other_slurm_jobs.append(job)

    prefixes = [job.prefix for job in info.slurm_jobs]
    if len(prefixes) != len(set(prefixes)):
        plan.warnings.append(
            "Several SLURM jobs for the same batch; the most recent one "
            "would be adopted."
        )
    if any(job.subfolder in ctx.later_subfolders for job in found_jobs):
        return _refuse(plan, "Found SLURM jobs for tasks not started yet.")
    if (
        plan.case
        in (
            ResumeCase.NO_HISTORY,
            ResumeCase.NEXT_TASK_NOT_STARTED,
            ResumeCase.RUN_WITHOUT_UNITS,
        )
        and info.slurm_jobs
    ):
        # A resume of this task deleted its run and was interrupted before
        # the rerun created new units
        if plan.previously_resumed_task_index != info.task_index:
            return _refuse(plan, "Found SLURM jobs for a task without units.")
        plan.warnings.append(
            "SLURM jobs from an interrupted resume of this task; they are "
            "adopted only if their script matches."
        )

    if plan.case == ResumeCase.UNITS_FINISHED:
        outputs = _read_task_outputs(
            fractal_ssh=fractal_ssh,
            folder=f"{ctx.remote_root}/{info.subfolder}",
        )
        task_output = _task_output_from_files(
            outputs=outputs,
            task_type=ctx.task_type,
            unit_selection=ctx.unit_selection or [],
        )
        reflected = _outputs_reflected(
            task_output=task_output,
            images=ctx.dataset_images,
            output_types=ctx.output_types,
        )
        info.outputs_reflected_in_dataset = reflected
        if not reflected:
            # Results not saved: rerun the task and adopt its SLURM jobs
            if info.selection_matches_units is False:
                _refuse(
                    plan,
                    "Images selected now differ from the finished units.",
                )
        elif ctx.any_unit_failed:
            plan.action = ResumeAction.MARK_FAILED
            plan.warnings.append(
                "Results already saved; mark the task run and job failed."
            )
        else:
            plan.warnings.append(
                "Results already saved; mark the task run done and skip "
                "the accounting record."
            )
            plan.type_filters.update(ctx.output_types)
            if info.task_index == ctx.last_task_index:
                plan.action = ResumeAction.MARK_DONE
                plan.resume_from_task_index = None
            else:
                plan.resume_from_task_index = info.task_index + 1


def _read_task_outputs(
    *, fractal_ssh: FractalSSH, folder: str
) -> dict[str, Any]:
    """
    Read all `*-output.json` files of a remote task subfolder.
    """
    names = fractal_ssh.run_command(cmd=f"ls -1 {folder}").split()
    return {
        name: fractal_ssh.read_remote_json_file(f"{folder}/{name}")
        for name in names
        if name.endswith("-output.json")
    }


def _task_output_from_files(
    *,
    outputs: dict[str, Any],
    task_type: str,
    unit_selection: list[str],
) -> TaskOutput:
    """
    Rebuild the merged task output, as the engine does after execution.
    """
    if task_type in (TaskType.NON_PARALLEL, TaskType.CONVERTER_NON_PARALLEL):
        prefix = f"{SUBMIT_PREFIX}-"
    else:
        # Parallel and compound-compute outputs (compound init is skipped)
        prefix = f"{MULTISUBMIT_PREFIX}-"
    task_outputs = []
    for name in sorted(outputs):
        if not name.startswith(prefix):
            continue
        success, result = outputs[name][0], outputs[name][1]
        if not success:
            continue
        try:
            task_outputs.append(
                TaskOutput(**result) if result is not None else TaskOutput()
            )
        except (TypeError, ValidationError):
            continue
    if not task_outputs:
        return TaskOutput()
    merged = merge_outputs(task_outputs)
    if merged.image_list_updates == [] and merged.image_list_removals == []:
        # Same as the engine: flag all input images as modified
        merged = TaskOutput(
            image_list_updates=[dict(zarr_url=url) for url in unit_selection]
        )
    return merged


def _outputs_reflected(
    *,
    task_output: TaskOutput,
    images: list[dict[str, Any]],
    output_types: dict[str, bool],
) -> bool:
    """
    Whether the dataset images already include the task output.
    """
    images_by_url = {img["zarr_url"]: img for img in images}
    if any(url in images_by_url for url in task_output.image_list_removals):
        return False
    for update in task_output.image_list_updates:
        image = images_by_url.get(update.zarr_url)
        if image is None:
            return False
        for key, value in update.attributes.items():
            if value is None:
                if key in image["attributes"]:
                    return False
            elif image["attributes"].get(key) != value:
                return False
        expected_types = {**update.types, **output_types}
        if any(image["types"].get(k) != v for k, v in expected_types.items()):
            return False
        if update.origin not in (None, update.zarr_url) and (
            image.get("origin") != update.origin
        ):
            return False
    return True
