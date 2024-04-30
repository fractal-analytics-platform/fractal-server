import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import JobV2
from ....schemas.v2.dataset import WorkflowTaskStatusTypeV2
from ....schemas.v2.status import StatusReadV2
from ....security import current_active_user
from ....security import User
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner
from fractal_server.app.runner.filenames import HISTORY_FILENAME

router = APIRouter()

logger = set_logger(__name__)


@router.get(
    "/project/{project_id}/status/",
    response_model=StatusReadV2,
)
async def get_workflowtask_status(
    project_id: int,
    dataset_id: int,
    workflow_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[StatusReadV2]:
    """
    Extract the status of all `WorkflowTaskV2` of a given `WorkflowV2` that ran
    on a given `DatasetV2`.

    *NOTE*: the current endpoint is not guaranteed to provide consistent
    results if the workflow task list is modified in a non-trivial way
    (that is, by adding intermediate tasks, removing tasks, or changing their
    order). See fractal-server GitHub issues: 793, 1083.
    """
    # Get the dataset DB entry
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]

    # Get the workflow DB entry
    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    # Check whether there exists a submitted job associated to this
    # workflow/dataset pair. If it does exist, it will be used later.
    # If there are multiple jobs, raise an error.
    stm = _get_submitted_jobs_statement()
    stm = stm.where(JobV2.dataset_id == dataset_id)
    stm = stm.where(JobV2.workflow_id == workflow_id)
    res = await db.execute(stm)
    running_jobs = res.scalars().all()
    if len(running_jobs) == 0:
        running_job = None
    elif len(running_jobs) == 1:
        running_job = running_jobs[0]
    else:
        string_ids = str([job.id for job in running_jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot get WorkflowTaskV2 statuses as DatasetV2 {dataset.id}"
                f" is linked to multiple active jobs: {string_ids}."
            ),
        )

    # Initialize empty dictionary for WorkflowTaskV2 status
    workflow_tasks_status_dict: dict = {}

    # Lowest priority: read status from DB, which corresponds to jobs that are
    # not running
    history = dataset.history
    for history_item in history:
        wftask_id = history_item["workflowtask"]["id"]
        wftask_status = history_item["status"]
        workflow_tasks_status_dict[wftask_id] = wftask_status

    if running_job is None:
        # If no job is running, the chronological-last history item is also the
        # positional-last workflow task to be included in the response.
        if len(dataset.history) > 0:
            last_valid_wftask_id = dataset.history[-1]["workflowtask"]["id"]
        else:
            last_valid_wftask_id = None
    else:
        # If a job is running, then gather more up-to-date information

        # Mid priority: Set all WorkflowTask's that are part of the running job
        # as "submitted"
        start = running_job.first_task_index
        end = running_job.last_task_index + 1
        for wftask in workflow.task_list[start:end]:
            workflow_tasks_status_dict[
                wftask.id
            ] = WorkflowTaskStatusTypeV2.SUBMITTED

        # The last workflow task that is included in the submitted job is also
        # the positional-last workflow task to be included in the response.
        try:
            last_valid_wftask_id = workflow.task_list[end - 1].id
        except IndexError as e:
            logger.warning(
                f"Handled IndexError in `get_workflowtask_status` ({str(e)})."
            )
            logger.warning(
                "Additional information: "
                f"{running_job.first_task_index=}; "
                f"{running_job.last_task_index=}; "
                f"{len(workflow.task_list)=}; "
                f"{dataset_id=}; "
                f"{workflow_id=}."
            )
            last_valid_wftask_id = None
            logger.warning(f"Now setting {last_valid_wftask_id=}.")

        # Highest priority: Read status updates coming from the running-job
        # temporary file. Note: this file only contains information on
        # WorkflowTask's that ran through successfully.
        tmp_file = Path(running_job.working_dir) / HISTORY_FILENAME
        try:
            with tmp_file.open("r") as f:
                history = json.load(f)
        except FileNotFoundError:
            history = []
        for history_item in history:
            wftask_id = history_item["workflowtask"]["id"]
            wftask_status = history_item["status"]
            workflow_tasks_status_dict[wftask_id] = wftask_status

    # Based on previously-gathered information, clean up the response body
    clean_workflow_tasks_status_dict = {}
    for wf_task in workflow.task_list:
        wf_task_status = workflow_tasks_status_dict.get(wf_task.id)
        if wf_task_status is None:
            # If a wftask ID was not found, ignore it and continue
            continue
        clean_workflow_tasks_status_dict[wf_task.id] = wf_task_status
        if wf_task_status == WorkflowTaskStatusTypeV2.FAILED:
            # Starting from the beginning of `workflow.task_list`, stop the
            # first time that you hit a failed job
            break
        if wf_task.id == last_valid_wftask_id:
            # Starting from the beginning of `workflow.task_list`, stop the
            # first time that you hit `last_valid_wftask_id``
            break

    response_body = StatusReadV2(status=clean_workflow_tasks_status_dict)
    return response_body
