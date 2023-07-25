from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ....common.schemas import ApplyWorkflowCreate
from ....common.schemas import ApplyWorkflowRead
from ....common.schemas import ProjectCreate
from ....common.schemas import ProjectRead
from ....common.schemas import ProjectUpdate
from ....config import get_settings
from ....logger import close_logger
from ....logger import set_logger
from ....syringe import Inject
from ...db import AsyncSession
from ...db import DBSyncSession
from ...db import get_db
from ...db import get_sync_db
from ...models import ApplyWorkflow
from ...models import Dataset
from ...models import JobStatusType
from ...models import LinkUserProject
from ...models import Project
from ...runner import submit_workflow
from ...runner import validate_workflow_compatibility
from ...runner.common import set_start_and_last_task_index
from ...security import current_active_user
from ...security import User
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner


router = APIRouter()


@router.get("/", response_model=list[ProjectRead])
async def get_list_project(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
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
    db: AsyncSession = Depends(get_db),
) -> Optional[ProjectRead]:
    """
    Create new poject
    """

    # Check that there is no project with the same user and name
    stm = (
        select(Project)
        .join(LinkUserProject)
        .where(Project.name == project.name)
        .where(LinkUserProject.user_id == user.id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Project name ({project.name}) already in use",
        )

    db_project = Project.from_orm(project)
    db_project.dataset_list.append(Dataset(name=project.default_dataset_name))
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


@router.get("/{project_id}", response_model=ProjectRead)
async def read_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ProjectRead]:
    """
    Return info on an existing project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.close()
    return project


@router.patch("/{project_id}", response_model=ProjectRead)
async def update_project(
    project_id: int,
    project_update: ProjectUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
):
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )

    # Check that there is no project with the same user and name
    stm = (
        select(Project)
        .join(LinkUserProject)
        .where(Project.name == project_update.name)
        .where(LinkUserProject.user_id == user.id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Project name ({project_update.name}) already in use",
        )

    for key, value in project_update.dict(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


@router.delete("/{project_id}", status_code=204)
async def delete_project(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete project
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    await db.delete(project)
    await db.commit()
    await db.close()
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
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
    db_sync: DBSyncSession = Depends(
        get_sync_db
    ),  # FIXME: why both sync and async?  # noqa
) -> Optional[ApplyWorkflowRead]:
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=input_dataset_id,
        user_id=user.id,
        db=db,
    )
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

    # Check that no other job with the same output_dataset_id is either
    # SUBMITTED or RUNNING
    stm = (
        select(ApplyWorkflow)
        .where(ApplyWorkflow.output_dataset_id == output_dataset_id)
        .where(
            ApplyWorkflow.status.in_(
                [JobStatusType.SUBMITTED, JobStatusType.RUNNING]
            )
        )
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Output dataset {output_dataset_id} is already in use "
                "in pending/running job(s)."
            ),
        )

    # Add new ApplyWorkflow object to DB
    job = ApplyWorkflow(
        project_id=project_id,
        input_dataset_id=input_dataset_id,
        output_dataset_id=output_dataset_id,
        workflow_id=workflow_id,
        workflow_dump=dict(
            workflow.dict(exclude={"task_list"}),
            task_list=[
                dict(wf_task.task.dict(exclude={"task"}), task=wf_task.dict())
                for wf_task in workflow.task_list
            ],
        ),
        **apply_workflow.dict(),
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
    db_sync.close()

    return job
