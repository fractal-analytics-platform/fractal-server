from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from ....config import get_settings
from ....logger import close_logger
from ....logger import set_logger
from ....syringe import Inject
from ...db import AsyncSession
from ...db import DBSyncSession
from ...db import get_db
from ...db import get_sync_db
from ...models import ApplyWorkflow
from ...models import ApplyWorkflowCreate
from ...models import ApplyWorkflowRead
from ...models import Dataset
from ...models import DatasetCreate
from ...models import DatasetRead
from ...models import DatasetUpdate
from ...models import LinkUserProject
from ...models import Project
from ...models import ProjectCreate
from ...models import ProjectRead
from ...models import ProjectUpdate
from ...models import Resource
from ...models import ResourceCreate
from ...models import ResourceRead
from ...models import ResourceUpdate
from ...models import Task
from ...models import Workflow
from ...models import WorkflowImport
from ...models import WorkflowRead
from ...models import WorkflowTaskCreate
from ...runner import auto_output_dataset
from ...runner import submit_workflow
from ...runner import validate_workflow_compatibility
from ...security import current_active_user
from ...security import User
from ._aux_functions import _check_workflow_exists
from ._aux_functions import _get_dataset_check_owner
from ._aux_functions import _get_project_check_owner
from ._aux_functions import _get_workflow_check_owner


router = APIRouter()


# Main endpoints (no ID required)


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
    db_project.user_member_list.append(user)
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


# Project endpoints ("/{project_id}")


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
    output_dataset_id: Optional[int] = None,
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
    project = output["project"]

    workflow = await _get_workflow_check_owner(
        project_id=project_id, workflow_id=workflow_id, user_id=user.id, db=db
    )

    if not workflow.task_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(f"Workflow {workflow_id} has empty task list"),
        )

    if output_dataset_id:
        output = await _get_dataset_check_owner(
            project_id=project_id,
            dataset_id=output_dataset_id,
            user_id=user.id,
            db=db,
        )
        output_dataset = output["dataset"]
    else:
        try:
            output_dataset = await auto_output_dataset(
                project=project,
                input_dataset=input_dataset,
                workflow=workflow,
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Could not determine output dataset. "
                    f"Original error: {str(e)}."
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
    if not input_dataset.resource_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Input dataset has empty resource_list",
        )
    try:
        validate_workflow_compatibility(
            workflow=workflow,
            input_dataset=input_dataset,
            output_dataset=output_dataset,
        )
    except TypeError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )

    job = ApplyWorkflow(
        project_id=project_id,
        input_dataset_id=input_dataset_id,
        output_dataset_id=output_dataset_id,
        workflow_id=workflow_id,
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


@router.get("/{project_id}", response_model=ProjectRead)
async def get_project(
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
    "/{project_id}/dataset/",
    response_model=DatasetRead,
    status_code=status.HTTP_201_CREATED,
)
async def add_dataset(
    project_id: int,
    dataset: DatasetCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[DatasetRead]:
    """
    Add new dataset to current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    db_dataset = Dataset(project_id=project_id, **dataset.dict())
    db.add(db_dataset)
    await db.commit()
    await db.refresh(db_dataset)
    await db.close()

    return db_dataset


@router.get("/{project_id}/workflow/", response_model=list[WorkflowRead])
async def get_workflow_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[WorkflowRead]]:
    """
    Get list of workflows associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(Workflow).where(Workflow.project_id == project_id)
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    await db.close()
    return workflow_list


@router.patch("/{project_id}", response_model=ProjectRead)
async def edit_project(
    project_id: int,
    project_update: ProjectUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
):
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    for key, value in project_update.dict(exclude_unset=True).items():
        setattr(project, key, value)

    await db.commit()
    await db.refresh(project)
    await db.close()
    return project


# Dataset endpoints ("/{project_id}/dataset/{dataset_id}")


@router.get("/{project_id}/dataset/{dataset_id}", response_model=DatasetRead)
async def get_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[DatasetRead]:
    """
    Get info on a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id, dataset_id=dataset_id, user_id=user.id, db=db
    )
    dataset = output["dataset"]
    await db.close()
    return dataset


@router.patch("/{project_id}/dataset/{dataset_id}", response_model=DatasetRead)
async def patch_dataset(
    project_id: int,
    dataset_id: int,
    dataset_update: DatasetUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[DatasetRead]:
    """
    Edit a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    db_dataset = output["dataset"]

    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(db_dataset, key, value)

    await db.commit()
    await db.refresh(db_dataset)
    await db.close()
    return db_dataset


@router.delete("/{project_id}/dataset/{dataset_id}", status_code=204)
async def delete_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a dataset associated to the current project
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    await db.delete(dataset)
    await db.commit()
    await db.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{project_id}/dataset/{dataset_id}/resource/",
    response_model=ResourceRead,
    status_code=status.HTTP_201_CREATED,
)
async def add_resource(
    project_id: int,
    dataset_id: int,
    resource: ResourceCreate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ResourceRead]:
    """
    Add resource to an existing dataset
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    db_resource = Resource(dataset_id=dataset.id, **resource.dict())
    db.add(db_resource)
    await db.commit()
    await db.refresh(db_resource)
    await db.close()
    return db_resource


@router.get(
    "/{project_id}/dataset/{dataset_id}/resource/",
    response_model=list[ResourceRead],
)
async def get_resource(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[list[ResourceRead]]:
    """
    Get resources from a dataset
    """
    await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    stm = select(Resource).where(Resource.dataset_id == dataset_id)
    res = await db.execute(stm)
    resource_list = res.scalars().all()
    await db.close()
    return resource_list


# Resource endpoints
# ("/{project_id}/dataset/{dataset_id}/resource/{resource_id}")


@router.delete(
    "/{project_id}/dataset/{dataset_id}/resource/{resource_id}",
    status_code=204,
)
async def delete_resource(
    project_id: int,
    dataset_id: int,
    resource_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a resource of a dataset
    """
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    resource = await db.get(Resource, resource_id)
    if not resource or resource.dataset_id not in (
        ds.id for ds in project.dataset_list
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Resource does not exist or does not belong to project",
        )
    await db.delete(resource)
    await db.commit()
    await db.close()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{project_id}/dataset/{dataset_id}/resource/{resource_id}",
    response_model=ResourceRead,
)
async def edit_resource(
    project_id: int,
    dataset_id: int,
    resource_id: int,
    resource_update: ResourceUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[ResourceRead]:
    """
    Edit a resource of a dataset
    """
    output = await _get_dataset_check_owner(
        project_id=project_id,
        dataset_id=dataset_id,
        user_id=user.id,
        db=db,
    )
    dataset = output["dataset"]
    orig_resource = await db.get(Resource, resource_id)

    if orig_resource not in dataset.resource_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Resource {resource_id} is not part of "
                f"dataset {dataset_id}"
            ),
        )

    for key, value in resource_update.dict(exclude_unset=True).items():
        setattr(orig_resource, key, value)
    await db.commit()
    await db.refresh(orig_resource)
    await db.close()
    return orig_resource


@router.post(
    "/{project_id}/workflow/import/",
    response_model=WorkflowRead,
    status_code=status.HTTP_201_CREATED,
)
async def import_workflow_into_project(
    project_id: int,
    workflow: WorkflowImport,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[WorkflowRead]:
    """
    Import an existing workflow into a project

    Also create all required objects (i.e. Workflow and WorkflowTask's) along
    the way.
    """

    # Preliminary checks
    await _get_project_check_owner(
        project_id=project_id,
        user_id=user.id,
        db=db,
    )

    await _check_workflow_exists(
        name=workflow.name, project_id=project_id, db=db
    )

    # Check that all required tasks are available
    # NOTE: by now we go through the pair (source, name), but later on we may
    # combine them into source -- see issue #293.
    tasks = [wf_task.task for wf_task in workflow.task_list]
    sourcename_to_id = {}
    for task in tasks:
        source = task.source
        name = task.name
        if not (source, name) in sourcename_to_id.keys():
            stm = select(Task).where(Task.source == source)
            tasks_by_source = (await db.execute(stm)).scalars().all()
            if not tasks_by_source:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(f"Found 0 tasks with {source=}."),
                )
            else:
                stm = (
                    select(Task)
                    .where(Task.source == source)
                    .where(Task.name == name)
                )
                current_task = (await db.execute(stm)).scalars().all()
                if len(current_task) != 1:
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            f"Found {len(current_task)} tasks with "
                            f"{name =} and {source=}."
                        ),
                    )
                sourcename_to_id[(source, name)] = current_task[0].id

    # Create new Workflow (with empty task_list)
    db_workflow = Workflow(
        project_id=project_id,
        **workflow.dict(exclude_none=True, exclude={"task_list"}),
    )
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # Insert tasks
    async with db:
        for _, wf_task in enumerate(workflow.task_list):
            # Identify task_id
            source = wf_task.task.source
            name = wf_task.task.name
            task_id = sourcename_to_id[(source, name)]
            # Prepare new_wf_task
            new_wf_task = WorkflowTaskCreate(
                **wf_task.dict(exclude_none=True),
            )
            # Insert task
            await db_workflow.insert_task(
                **new_wf_task.dict(),
                task_id=task_id,
                db=db,
            )

    await db.close()
    return db_workflow
