"""
Auxiliary functions to get object from the database or perform simple checks
"""

from typing import Any
from typing import TypedDict

from fastapi import HTTPException
from fastapi import status
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select
from sqlmodel.sql.expression import SelectOfScalar

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ProjectPermissions
from fractal_server.logger import set_logger

logger = set_logger(__name__)


async def _get_project_check_access(
    *,
    project_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> ProjectV2:
    """
    Check that user is a member of project and return the project.

    Args:
        project_id:
        user_id:
        required_permissions:
        db:

    Returns:
        The project object

    Raises:
        HTTPException(status_code=403_FORBIDDEN):
            - If the user is not a member of the project;
            - If the user has not accepted the invitation yet;
            - If the user has not the target permissions.
        HTTPException(status_code=404_NOT_FOUND):
            If the project does not exist
    """
    project = await db.get(ProjectV2, project_id)
    if project is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )

    link_user_project = await db.get(LinkUserProjectV2, (project_id, user_id))
    if (
        link_user_project is None
        or not link_user_project.is_verified
        or required_permissions not in link_user_project.permissions
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "You are not authorized to perform this action. "
                "If you think this is by mistake, "
                "please contact the project owner."
            ),
        )

    return project


async def _get_workflow_check_access(
    *,
    workflow_id: int,
    project_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> WorkflowV2:
    """
    Get a workflow and a project, after access control on the project.

    Args:
        workflow_id:
        project_id:
        user_id:
        db:

    Returns:
        The workflow object.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If the project or the workflow do not exist or if they are not
            associated
        HTTPException(status_code=403_FORBIDDEN):
            If the user is not a member of the project
    """

    # Access control for project
    await _get_project_check_access(
        project_id=project_id,
        user_id=user_id,
        required_permissions=required_permissions,
        db=db,
    )

    res = await db.execute(
        select(WorkflowV2)
        .where(WorkflowV2.id == workflow_id)
        .where(WorkflowV2.project_id == project_id)
        .execution_options(populate_existing=True)  # See issue 1087
    )
    workflow = res.scalars().one_or_none()

    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Workflow not found"
        )

    return workflow


async def _get_workflow_task_check_access(
    *,
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> tuple[WorkflowTaskV2, WorkflowV2]:
    """
    Check that user has access to Workflow and WorkflowTask.

    Args:
        project_id:
        workflow_id:
        workflow_task_id:
        user_id:
        db:

    Returns:
        Tuple of WorkflowTask and Workflow objects.

    Raises:
        HTTPException(status_code=404_NOT_FOUND):
            If the project, the workflow or the workflowtask do not exist or
            if they are not associated
        HTTPException(status_code=403_FORBIDDEN):
            If the user is not a member of the project
    """

    # Access control for workflow
    workflow = await _get_workflow_check_access(
        workflow_id=workflow_id,
        project_id=project_id,
        user_id=user_id,
        required_permissions=required_permissions,
        db=db,
    )

    res = await db.execute(
        select(WorkflowTaskV2)
        .where(WorkflowTaskV2.id == workflow_task_id)
        .where(WorkflowTaskV2.workflow_id == workflow_id)
    )
    workflow_task = res.scalars().one_or_none()

    if workflow_task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="WorkflowTask not found",
        )

    return workflow_task, workflow


async def _check_workflow_exists(
    *,
    name: str,
    project_id: int,
    db: AsyncSession,
) -> None:
    """
    Check that no other workflow of this project has the same name.

    Args:
        name: Workflow name
        project_id: Project ID
        db:

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If such a workflow already exists
    """
    stm = (
        select(WorkflowV2)
        .where(WorkflowV2.name == name)
        .where(WorkflowV2.project_id == project_id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Workflow with {name=} and {project_id=} already exists.",
        )


async def _check_project_exists(
    *,
    project_name: str,
    user_id: int,
    db: AsyncSession,
) -> None:
    """
    Check that no other project with this name exists for this user.

    Args:
        project_name: Project name
        user_id: User ID
        db:

    Raises:
        HTTPException(status_code=422_UNPROCESSABLE_ENTITY):
            If such a project already exists
    """
    stm = (
        select(ProjectV2)
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .where(ProjectV2.name == project_name)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Project name ({project_name}) already in use",
        )


class DatasetOrProject(TypedDict):
    dataset: DatasetV2
    project: ProjectV2


async def _get_dataset_check_access(
    *,
    project_id: int,
    dataset_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> DatasetOrProject:
    """
    Get a dataset and a project, after access control on the project

    Args:
        project_id:
        dataset_id:
        user_id:
        db:

    Returns:
        Dictionary with the dataset and project objects (keys: `dataset`,
            `project`).

    Raises:
        HTTPException(status_code=404_UNPROCESSABLE_ENTITY):
            If the project or the dataset do not exist or if they are not
            associated
        HTTPException(status_code=403_FORBIDDEN):
            If the user is not a member of the project
    """
    # Access control for project
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user_id,
        required_permissions=required_permissions,
        db=db,
    )

    res = await db.execute(
        select(DatasetV2)
        .where(DatasetV2.id == dataset_id)
        .where(DatasetV2.project_id == project_id)
        .execution_options(populate_existing=True)  # See issue 1087
    )
    dataset = res.scalars().one_or_none()

    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
        )

    return dict(dataset=dataset, project=project)


class JobAndProject(TypedDict):
    job: JobV2
    project: ProjectV2


async def _get_job_check_access(
    *,
    project_id: int,
    job_id: int,
    user_id: int,
    required_permissions: ProjectPermissions,
    db: AsyncSession,
) -> JobAndProject:
    """
    Get a job and a project, after access control on the project

    Args:
        project_id:
        job_id:
        user_id:
        db:

    Returns:
        Dictionary with the job and project objects (keys: `job`,
            `project`).

    Raises:
        HTTPException(status_code=404_UNPROCESSABLE_ENTITY):
            If the project or the job do not exist or if they are not
            associated
        HTTPException(status_code=403_FORBIDDEN):
            If the user is not a member of the project
    """
    # Access control for project
    project = await _get_project_check_access(
        project_id=project_id,
        user_id=user_id,
        required_permissions=required_permissions,
        db=db,
    )

    res = await db.execute(
        select(JobV2)
        .where(JobV2.id == job_id)
        .where(JobV2.project_id == project_id)
    )
    job = res.scalars().one_or_none()

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
        )

    return dict(job=job, project=project)


def _get_submitted_jobs_statement() -> SelectOfScalar:
    """
    Returns:
        A sqlmodel statement that selects all `Job`s with
        `Job.status` equal to `submitted`.
    """
    stm = select(JobV2).where(JobV2.status == JobStatusType.SUBMITTED)
    return stm


async def _workflow_has_submitted_job(
    workflow_id: int,
    db: AsyncSession,
) -> bool:
    res = await db.execute(
        select(JobV2.id)
        .where(JobV2.status == JobStatusType.SUBMITTED)
        .where(JobV2.workflow_id == workflow_id)
        .limit(1)
    )
    submitted_jobs = res.scalar_one_or_none()
    if submitted_jobs is not None:
        return True

    return False


async def _workflow_insert_task(
    *,
    workflow_id: int,
    task_id: int,
    meta_parallel: dict[str, Any] | None = None,
    meta_non_parallel: dict[str, Any] | None = None,
    args_non_parallel: dict[str, Any] | None = None,
    args_parallel: dict[str, Any] | None = None,
    type_filters: dict[str, bool] | None = None,
    db: AsyncSession,
) -> WorkflowTaskV2:
    """
    Insert a new WorkflowTask into Workflow.task_list

    Args:
        workflow_id:
        task_id:

        meta_parallel:
        meta_non_parallel:
        args_non_parallel:
        args_parallel:
        type_filters:
        db:
    """
    db_workflow = await db.get(WorkflowV2, workflow_id)
    if db_workflow is None:
        raise ValueError(f"Workflow {workflow_id} does not exist")

    # Get task from db
    db_task = await db.get(TaskV2, task_id)
    if db_task is None:
        raise ValueError(f"TaskV2 {task_id} not found.")
    task_type = db_task.type

    # Combine meta_parallel (higher priority)
    # and db_task.meta_parallel (lower priority)
    final_meta_parallel = (db_task.meta_parallel or {}).copy()
    final_meta_parallel.update(meta_parallel or {})
    if final_meta_parallel == {}:
        final_meta_parallel = None
    # Combine meta_non_parallel (higher priority)
    # and db_task.meta_non_parallel (lower priority)
    final_meta_non_parallel = (db_task.meta_non_parallel or {}).copy()
    final_meta_non_parallel.update(meta_non_parallel or {})
    if final_meta_non_parallel == {}:
        final_meta_non_parallel = None

    # Create DB entry
    wf_task = WorkflowTaskV2(
        task_type=task_type,
        task_id=task_id,
        args_non_parallel=args_non_parallel,
        args_parallel=args_parallel,
        meta_parallel=final_meta_parallel,
        meta_non_parallel=final_meta_non_parallel,
        type_filters=(type_filters or dict()),
    )
    db_workflow.task_list.append(wf_task)
    flag_modified(db_workflow, "task_list")
    await db.commit()

    wf_task = await db.get(
        WorkflowTaskV2,
        wf_task.id,
        populate_existing=True,  # See issue 1087
    )

    return wf_task


async def clean_app_job_list(
    db: AsyncSession,
    jobs_list: list[int],
) -> list[int]:
    """
    Remove from a job list all jobs with status different from submitted.

    Args:
        db: Async database session
        jobs_list: List of job IDs currently associated to the app.

    Return:
        List of IDs for submitted jobs.
    """
    logger.info(f"[clean_app_job_list] START - {jobs_list=}.")
    stmt = select(JobV2).where(JobV2.id.in_(jobs_list))
    result = await db.execute(stmt)
    db_jobs_list = result.scalars().all()
    submitted_job_ids = [
        job.id for job in db_jobs_list if job.status == JobStatusType.SUBMITTED
    ]
    logger.info(f"[clean_app_job_list] END - {submitted_job_ids=}.")
    return submitted_job_ids


async def _get_dataset_or_404(
    *,
    dataset_id: int,
    db: AsyncSession,
) -> DatasetV2:
    """
    Get a dataset or raise 404.

    Args:
        dataset_id:
        db:
    """
    ds = await db.get(DatasetV2, dataset_id)
    if ds is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found.",
        )
    else:
        return ds


async def _get_workflow_or_404(
    *,
    workflow_id: int,
    db: AsyncSession,
) -> WorkflowV2:
    """
    Get a workflow or raise 404.

    Args:
        workflow_id:
        db:
    """
    wf = await db.get(WorkflowV2, workflow_id)
    if wf is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow {workflow_id} not found.",
        )
    else:
        return wf


async def _get_workflowtask_or_404(
    *,
    workflowtask_id: int,
    db: AsyncSession,
) -> WorkflowTaskV2:
    """
    Get a workflow task or raise 404.

    Args:
        workflowtask_id:
        db:
    """
    wftask = await db.get(WorkflowTaskV2, workflowtask_id)
    if wftask is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"WorkflowTask {workflowtask_id} not found.",
        )
    else:
        return wftask


async def _get_user_resource_id(user_id: int, db: AsyncSession) -> int | None:
    res = await db.execute(
        select(Resource.id)
        .join(Profile, Resource.id == Profile.resource_id)
        .join(UserOAuth, Profile.id == UserOAuth.profile_id)
        .where(UserOAuth.id == user_id)
    )
    resource_id = res.scalar_one_or_none()
    return resource_id
