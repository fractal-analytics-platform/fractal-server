from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from .....config import get_settings
from .....logger import close_logger
from .....logger import set_logger
from .....syringe import Inject
from ....db import AsyncSession
from ....db import get_async_db
from ....models import ApplyWorkflow
from ....models import Dataset
from ....models import LinkUserProject
from ....models import Project
from ....models import Workflow
from ....runner import submit_workflow
from ....runner import validate_workflow_compatibility
from ....runner.common import set_start_and_last_task_index
from ....schemas import ApplyWorkflowCreate
from ....schemas import ApplyWorkflowRead
from ....schemas import JobStatusType
from ....schemas import ProjectCreate
from ....schemas import ProjectRead
from ....schemas import ProjectUpdate
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from ._aux_functions import _check_project_exists
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_submitted_jobs_statement
from ._aux_functions import _get_workflow_check_owner

router = APIRouter()


def _encode_as_utc(dt: datetime):
    return dt.replace(tzinfo=timezone.utc).isoformat()


@router.get("/", response_model=list[ProjectRead])
async def get_list_project(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[Project]:
    """
    Return list of projects user is member of
    """
    stm = (
        select(Project)
        .join(LinkUserProject)
        .where(LinkUserProject.user_id == user.id)
    )
    res = await db.execute(stm)
    project_list = res.scalars().all()
    await db.close()
    return project_list


@router.post("/", response_model=ProjectRead, status_code=201)
async def create_project(
    project: ProjectCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ProjectRead]:
    """
    Create new poject
    """

    # Check that there is no project with the same user and name
    await _check_project_exists(
        project_name=project.name, user_id=user.id, db=db
    )

    db_project = Project(**project.dict())
    db_project.user_list.append(user)
    try:
        db.add(db_project)
        await db.commit()
        await db.refresh(db_project)
        await db.close()
    except IntegrityError as e:
        await db.rollback()
        logger = set_logger("create_project")
        logger.error(str(e))
        close_logger(logger)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

    return db_project


@router.get("/{project_id}/", response_model=ProjectRead)
async def read_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ProjectRead]:
    """
    Return info on an existing project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.close()
    return project


@router.patch("/{project_id}/", response_model=ProjectRead)
async def update_project(
    project_id: int,
    project_update: ProjectUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    # Check that there is no project with the same user and name
    if project_update.name is not None:
        await _check_project_exists(
            project_name=project_update.name, user_id=user.id, db=db
        )

    for key, value in project_update.dict(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


@router.delete("/{project_id}/", status_code=204)
async def delete_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> Response:
    """
    Delete project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    # Fail if there exist jobs that are submitted and in relation with the
    # current project.
    stm = _get_submitted_jobs_statement().where(
        ApplyWorkflow.project_id == project_id
    )
    res = await db.execute(stm)
    jobs = res.scalars().all()
    if jobs:
        string_ids = str([job.id for job in jobs])[1:-1]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot delete project {project.id} because it "
                f"is linked to active job(s) {string_ids}."
            ),
        )

    # Cascade operations

    # Workflows
    stm = select(Workflow).where(Workflow.project_id == project_id)
    res = await db.execute(stm)
    workflows = res.scalars().all()
    for wf in workflows:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current workflow
        stm = select(ApplyWorkflow).where(ApplyWorkflow.workflow_id == wf.id)
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            job.workflow_id = None
            await db.merge(job)
        await db.commit()
        # Delete workflow
        await db.delete(wf)

    # Dataset
    stm = select(Dataset).where(Dataset.project_id == project_id)
    res = await db.execute(stm)
    datasets = res.scalars().all()
    for ds in datasets:
        # Cascade operations: set foreign-keys to null for jobs which are in
        # relationship with the current dataset
        # input_dataset
        stm = select(ApplyWorkflow).where(
            ApplyWorkflow.input_dataset_id == ds.id
        )
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            job.input_dataset_id = None
            await db.merge(job)
        await db.commit()
        # output_dataset
        stm = select(ApplyWorkflow).where(
            ApplyWorkflow.output_dataset_id == ds.id
        )
        res = await db.execute(stm)
        jobs = res.scalars().all()
        for job in jobs:
            job.output_dataset_id = None
            await db.merge(job)
        await db.commit()
        await db.delete(ds)

    # Job
    stm = select(ApplyWorkflow).where(ApplyWorkflow.project_id == project_id)
    res = await db.execute(stm)
    jobs = res.scalars().all()
    for job in jobs:
        job.project_id = None
        await db.merge(job)

    await db.commit()

    await db.delete(project)
    await db.commit()

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{project_id}/workflow/{workflow_id}/apply/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ApplyWorkflowRead,
)
async def apply_workflow(
    project_id: int,
    workflow_id: int,
    apply_workflow: ApplyWorkflowCreate,
    background_tasks: BackgroundTasks,
    input_dataset_id: int,
    output_dataset_id: int,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> Optional[ApplyWorkflowRead]:

    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=input_dataset_id,
        user_id=user.id,
        db=db,
    )
    project = output["project"]
    input_dataset = output["dataset"]

    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=output_dataset_id,
        user_id=user.id,
        db=db,
    )
    output_dataset = output["dataset"]
    if output_dataset.read_only:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot apply workflow because output dataset "
                f"({output_dataset_id=}) is read_only."
            ),
        )

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    if not workflow.task_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Workflow {workflow_id} has empty task list",
        )

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    try:
        first_task_index, last_task_index = set_start_and_last_task_index(
            num_tasks,
            first_task_index=apply_workflow.first_task_index,
            last_task_index=apply_workflow.last_task_index,
        )
        apply_workflow.first_task_index = first_task_index
        apply_workflow.last_task_index = last_task_index
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Invalid values for first_task_index or last_task_index "
                f"(with {num_tasks=}).\n"
                f"Original error: {str(e)}"
            ),
        )

    # If backend is SLURM, check that the user has required attributes
    settings = Inject(get_settings)
    backend = settings.FRACTAL_RUNNER_BACKEND
    if backend == "slurm":
        if not user.slurm_user:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"FRACTAL_RUNNER_BACKEND={backend}, "
                    f"but {user.slurm_user=}."
                ),
            )
        if not user.cache_dir:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"FRACTAL_RUNNER_BACKEND={backend}, "
                    f"but {user.cache_dir=}."
                ),
            )

    # Check that datasets have the right number of resources
    if not input_dataset.resource_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Input dataset has empty resource_list",
        )
    if len(output_dataset.resource_list) != 1:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Output dataset must have a single resource, "
                f"but it has {len(output_dataset.resource_list)}"
            ),
        )

    try:
        validate_workflow_compatibility(
            workflow=workflow,
            input_dataset=input_dataset,
            output_dataset=output_dataset,
            first_task_index=apply_workflow.first_task_index,
            last_task_index=apply_workflow.last_task_index,
        )
    except TypeError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    # Check that no other job with the same output_dataset_id is SUBMITTED
    stm = (
        select(ApplyWorkflow)
        .where(ApplyWorkflow.output_dataset_id == output_dataset_id)
        .where(ApplyWorkflow.status == JobStatusType.SUBMITTED)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Output dataset {output_dataset_id} is already in use "
                "in submitted job(s)."
            ),
        )

    if apply_workflow.slurm_account is not None:
        if apply_workflow.slurm_account not in user.slurm_accounts:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"SLURM account '{apply_workflow.slurm_account}' is not "
                    "among those available to the current user"
                ),
            )
    else:
        if len(user.slurm_accounts) > 0:
            apply_workflow.slurm_account = user.slurm_accounts[0]

    # Add new ApplyWorkflow object to DB
    job = ApplyWorkflow(
        project_id=project_id,
        input_dataset_id=input_dataset_id,
        output_dataset_id=output_dataset_id,
        workflow_id=workflow_id,
        user_email=user.email,
        input_dataset_dump=dict(
            **input_dataset.model_dump(
                exclude={"resource_list", "timestamp_created"}
            ),
            timestamp_created=_encode_as_utc(input_dataset.timestamp_created),
            resource_list=[
                resource.model_dump()
                for resource in input_dataset.resource_list
            ],
        ),
        output_dataset_dump=dict(
            **output_dataset.model_dump(
                exclude={"resource_list", "timestamp_created"}
            ),
            timestamp_created=_encode_as_utc(output_dataset.timestamp_created),
            resource_list=[
                resource.model_dump()
                for resource in output_dataset.resource_list
            ],
        ),
        workflow_dump=dict(
            **workflow.model_dump(exclude={"task_list", "timestamp_created"}),
            timestamp_created=_encode_as_utc(workflow.timestamp_created),
            task_list=[
                dict(
                    **wf_task.model_dump(exclude={"task"}),
                    task=wf_task.task.model_dump(),
                )
                for wf_task in workflow.task_list
            ],
        ),
        project_dump=dict(
            **project.model_dump(exclude={"user_list", "timestamp_created"}),
            timestamp_created=_encode_as_utc(project.timestamp_created),
        ),
        **apply_workflow.dict(),
    )

    stm = (
        select(ApplyWorkflow)
        .where(ApplyWorkflow.project_id == project_id)
        .where(ApplyWorkflow.workflow_id == workflow_id)
        .where(ApplyWorkflow.input_dataset_id == input_dataset_id)
        .where(ApplyWorkflow.output_dataset_id == output_dataset_id)
        .order_by(ApplyWorkflow.start_timestamp.desc())
        .limit(1)
    )
    res = await db.execute(stm)
    db_job = res.scalar_one_or_none()
    if db_job and abs(
        db_job.start_timestamp.replace(tzinfo=timezone.utc)
        - job.start_timestamp
    ) < timedelta(seconds=settings.FRACTAL_API_SUBMIT_RATE_LIMIT):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"The endpoint 'POST /{project_id}/workflow/{workflow_id}/"
                "apply/' was called several times with an interval of less "
                f"than {settings.FRACTAL_API_SUBMIT_RATE_LIMIT} seconds, using"
                " the same foreign keys. If it was intentional, please wait "
                "and try again."
            ),
        )

    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(
        submit_workflow,
        workflow_id=workflow.id,
        input_dataset_id=input_dataset.id,
        output_dataset_id=output_dataset.id,
        job_id=job.id,
        worker_init=apply_workflow.worker_init,
        slurm_user=user.slurm_user,
        user_cache_dir=user.cache_dir,
    )

    await db.close()

    return job
