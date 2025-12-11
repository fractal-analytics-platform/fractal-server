import os
from pathlib import Path
from typing import Literal

from fastapi import HTTPException
from fastapi import status

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import WorkflowTaskV2
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_functions import _get_dataset_or_404
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_project_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import _get_workflow_or_404
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflowtask_or_404,
)
from fractal_server.app.schemas.v2.job import JobStatusType
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.logger import set_logger
from fractal_server.zip_tools import _read_single_file_from_zip

logger = set_logger(__name__)


async def get_history_unit_or_404(
    *, history_unit_id: int, db: AsyncSession
) -> HistoryUnit:
    """
    Get an existing HistoryUnit  or raise a 404.

    Args:
        history_unit_id: The `HistoryUnit` id
        db: An asynchronous db session
    """
    history_unit = await db.get(HistoryUnit, history_unit_id)
    if history_unit is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"HistoryUnit {history_unit_id} not found",
        )
    return history_unit


async def get_history_run_or_404(
    *, history_run_id: int, db: AsyncSession
) -> HistoryRun:
    """
    Get an existing HistoryRun  or raise a 404.

    Args:
        history_run_id:
        db:
    """
    history_run = await db.get(HistoryRun, history_run_id)
    if history_run is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"HistoryRun {history_run_id} not found",
        )
    return history_run


def read_log_file(
    *,
    task_name: str,
    dataset_id: int,
    logfile: str,
    job_working_dir: str,
    job_status: JobStatusType,
) -> str:
    """
    Returns the contents of a Job's log file, either directly from the working
    directory or from the corresponding ZIP archive.

    The function first checks if `logfile` exists on disk.

    If not, it checks if the Job working directory has been zipped and tries to
    read `logfile` from within the archive.
    (Note: it is assumed that `logfile` is relative to `job_working_dir`)
    """
    archive_path = os.path.normpath(job_working_dir) + ".zip"
    try:
        if Path(logfile).exists():
            with open(logfile) as f:
                return f.read()
        elif Path(archive_path).exists():
            relative_logfile = (
                Path(logfile).relative_to(job_working_dir).as_posix()
            )
            return _read_single_file_from_zip(
                file_path=relative_logfile, archive_path=archive_path
            )
        else:
            match job_status:
                case JobStatusType.SUBMITTED:
                    logger.info(
                        f"Neither {logfile=} nor {archive_path=} exist "
                        "(for submitted job)."
                    )
                case _:
                    logger.warning(
                        f"Error while retrieving logs for {logfile=} and "
                        f"{archive_path=}."
                    )
            return (
                f"Logs for task '{task_name}' in dataset "
                f"{dataset_id} are not available."
            )
    except Exception as e:
        logger.error(
            f"Error while retrieving logs for {logfile=} and {archive_path=}. "
            f"Original error: {str(e)}"
        )
        return (
            f"Error while retrieving logs for task '{task_name}' "
            f"in dataset {dataset_id}."
        )


async def _verify_workflow_and_dataset_access(
    *,
    project_id: int,
    workflow_id: int,
    dataset_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> dict[Literal["dataset", "workflow"], DatasetV2 | WorkflowV2]:
    """
    Verify user access to a dataset/workflow pair.

    Args:
        project_id:
        workflow_id:
        dataset_id:
        user_id:
        db:
    """
    await _get_project_check_access(
        project_id=project_id,
        user_id=user_id,
        required_permissions=required_permissions,
        db=db,
    )
    workflow = await _get_workflow_or_404(
        workflow_id=workflow_id,
        db=db,
    )
    if workflow.project_id != project_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Workflow does not belong to expected project.",
        )
    dataset = await _get_dataset_or_404(
        dataset_id=dataset_id,
        db=db,
    )
    if dataset.project_id != project_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Dataset does not belong to expected project.",
        )

    return dict(dataset=dataset, workflow=workflow)


async def get_wftask_check_access(
    *,
    project_id: int,
    dataset_id: int,
    workflowtask_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> WorkflowTaskV2:
    """
    Verify user access for the history of this dataset and workflowtask.

    Args:
        project_id:
        dataset_id:
        workflowtask_id:
        user_id:
        db:
    """
    wftask = await _get_workflowtask_or_404(
        workflowtask_id=workflowtask_id,
        db=db,
    )
    await _verify_workflow_and_dataset_access(
        project_id=project_id,
        dataset_id=dataset_id,
        workflow_id=wftask.workflow_id,
        required_permissions=required_permissions,
        user_id=user_id,
        db=db,
    )
    return wftask
