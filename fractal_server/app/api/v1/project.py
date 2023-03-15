import asyncio
import logging
import os
from pathlib import Path
from typing import List
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import UUID4
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

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
from ...models import WorkflowCreate
from ...models import WorkflowImport
from ...models import WorkflowRead
from ...models import WorkflowTaskCreate
from ...runner import auto_output_dataset
from ...runner import submit_workflow
from ...runner import validate_workflow_compatibility
from ...security import current_active_user
from ...security import User


router = APIRouter()


async def _get_project_check_owner(
    *,
    project_id: int,
    user_id: UUID4,
    db: AsyncSession = Depends(get_db),
) -> Project:
    """
    Check that user is a member of project and return

    Raises:
        HTTPException(status_code=403_FORBIDDEN): If the user is not a
                                                  member of the project
        HTTPException(status_code=404_NOT_FOUND): If the project does not
                                                  exist
    """
    project, link_user_project = await asyncio.gather(
        db.get(Project, project_id),
        db.get(LinkUserProject, (project_id, user_id)),
    )
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    if not link_user_project:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not allowed on project {project_id}",
        )
    return project


async def _get_dataset_check_owner(
    *,
    project_id: int,
    dataset_id: int,
    user_id: UUID4,
    db: AsyncSession = Depends(get_db),
) -> Dataset:
    """
    Check that user is a member of project and return

    Raises:
        HTTPException(status_code=403_FORBIDDEN): If the user is not a
                                                         member of the project
        HTTPException(status_code=404_NOT_FOUND): If the dataset or project do
                                                  not exist
    """
    project, dataset, link_user_project = await asyncio.gather(
        db.get(Project, project_id),
        db.get(Dataset, dataset_id),
        db.get(LinkUserProject, (project_id, user_id)),
    )
    if not project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Project not found"
        )
    if not link_user_project:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not allowed on project {project_id}",
        )
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found"
        )
    if dataset.project_id != project_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid {project_id=} for {dataset_id=}",
        )
    return dict(dataset=dataset, project=project)


# Main endpoints (no ID required)


@router.get("/", response_model=List[ProjectRead])
async def get_list_project(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[Project]:
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

    # Check that project_dir is an absolute path
    if not os.path.isabs(project.project_dir):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Project dir {project.project_dir} is not an absolute path"
            ),
        )

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
    except IntegrityError as e:
        await db.rollback()
        logging.error(str(e))
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(e),
        )

    return db_project


@router.post(
    "/apply/",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ApplyWorkflowRead,
)
async def apply_workflow(
    apply_workflow: ApplyWorkflowCreate,
    background_tasks: BackgroundTasks,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
    db_sync: DBSyncSession = Depends(get_sync_db),
) -> Optional[ApplyWorkflowRead]:

    output = await _get_dataset_check_owner(
        project_id=apply_workflow.project_id,
        dataset_id=apply_workflow.input_dataset_id,
        user_id=user.id,
        db=db,
    )
    input_dataset = output["dataset"]
    project = output["project"]

    workflow = db_sync.get(Workflow, apply_workflow.workflow_id)
    if not workflow:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Workflow {apply_workflow.workflow_id} not found",
        )
    if workflow.project_id != project.id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Error: {workflow.project_id=} differs from {project.id=}",
        )
    if not workflow.task_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Workflow {apply_workflow.workflow_id} has empty task list"
            ),
        )

    if apply_workflow.output_dataset_id:
        output = await _get_dataset_check_owner(
            project_id=apply_workflow.project_id,
            dataset_id=apply_workflow.output_dataset_id,
            user_id=user.id,
            db=db,
        )
        output_dataset = output["dataset"]
    else:
        try:
            output_dataset = await auto_output_dataset(
                project=project, input_dataset=input_dataset, workflow=workflow
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"Could not determine output dataset. "
                    f"Original error: {str(e)}."
                ),
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

    job = ApplyWorkflow.from_orm(apply_workflow)
    db.add(job)
    await db.commit()
    await db.refresh(job)

    background_tasks.add_task(
        submit_workflow,
        workflow=workflow,
        input_dataset=input_dataset,
        output_dataset=output_dataset,
        job_id=job.id,
        slurm_user=user.slurm_user,
        worker_init=apply_workflow.worker_init,
        project_dir=project.project_dir,
    )

    return job


# Project endpoints ("/{project_id}")


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
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{project_id}/",
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
    return db_dataset


@router.get("/{project_id}/workflows/", response_model=List[WorkflowRead])
async def get_workflow_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[List[WorkflowRead]]:
    """
    Get list of workflows associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(Workflow).where(Workflow.project_id == project_id)
    res = await db.execute(stm)
    workflow_list = res.scalars().all()
    return workflow_list


@router.get("/{project_id}/jobs/", response_model=List[ApplyWorkflowRead])
async def get_job_list(
    project_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[List[ApplyWorkflowRead]]:
    """
    Get list of jobs associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(ApplyWorkflow).where(ApplyWorkflow.project_id == project_id)
    res = await db.execute(stm)
    job_list = res.scalars().all()
    return job_list


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
    return project


# Dataset endpoints ("/{project_id}/{dataset_id}")


@router.get("/{project_id}/{dataset_id}", response_model=DatasetRead)
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
    return dataset


@router.patch("/{project_id}/{dataset_id}", response_model=DatasetRead)
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
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    db_dataset = await db.get(Dataset, dataset_id)
    if db_dataset not in project.dataset_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Dataset {dataset_id} is not part of project {project_id}",
        )

    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(db_dataset, key, value)

    await db.commit()
    await db.refresh(db_dataset)
    return db_dataset


@router.delete("/{project_id}/{dataset_id}", status_code=204)
async def delete_dataset(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """
    Delete a dataset associated to the current project
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = (
        select(Dataset)
        .join(Project)
        .where(Project.id == project_id)
        .where(Dataset.id == dataset_id)
    )
    res = await db.execute(stm)
    dataset = res.scalar()
    await db.delete(dataset)
    await db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{project_id}/{dataset_id}",
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

    # Check that path is absolute, which is needed for when the server submits
    # tasks as a different user
    if not Path(resource.path).is_absolute():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Path `{resource.path}` is not absolute.",
        )

    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    dataset = await db.get(Dataset, dataset_id)
    if dataset not in project.dataset_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Dataset {dataset_id} is not part of project {project_id}",
        )

    db_resource = Resource(dataset_id=dataset.id, **resource.dict())
    db.add(db_resource)
    await db.commit()
    await db.refresh(db_resource)
    return db_resource


@router.get(
    "/{project_id}/{dataset_id}/resources/",
    response_model=List[ResourceRead],
)
async def get_resource(
    project_id: int,
    dataset_id: int,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[List[ResourceRead]]:
    """
    Get resources from a dataset
    """
    await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    stm = select(Resource).where(Resource.dataset_id == dataset_id)
    res = await db.execute(stm)
    resource_list = res.scalars().all()
    return resource_list


# Resource endpoints ("/{project_id}/{dataset_id}/{resource_id}")


@router.delete("/{project_id}/{dataset_id}/{resource_id}", status_code=204)
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
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch(
    "/{project_id}/{dataset_id}/{resource_id}", response_model=ResourceRead
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
    project = await _get_project_check_owner(
        project_id=project_id, user_id=user.id, db=db
    )
    dataset = await db.get(Dataset, dataset_id)
    orig_resource = await db.get(Resource, resource_id)

    if dataset not in project.dataset_list:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Dataset {dataset_id} is not part of project {project_id}",
        )
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
    return orig_resource


@router.post(
    "/{project_id}/import-workflow/",
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
    stm = (
        select(Workflow)
        .where(Workflow.name == workflow.name)
        .where(Workflow.project_id == project_id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Workflow with name={workflow.name} and\
                    {project_id=} already in use",
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
            stm = (
                select(Task)
                .where(Task.name == name)
                .where(Task.source == source)
            )
            res = await db.execute(stm)
            current_task = res.scalars().all()
            if not len(current_task) == 1:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"Found {len(current_task)} tasks with {source=}."
                    ),
                )
            sourcename_to_id[(source, name)] = current_task[0].id

    # Create new Workflow
    workflow_create = WorkflowCreate(
        project_id=project_id, **workflow.dict(exclude_none=True)
    )
    db_workflow = Workflow.from_orm(workflow_create)
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
                workflow_id=db_workflow.id,
                task_id=task_id,
            )
            # Insert task
            await db_workflow.insert_task(
                **new_wf_task.dict(exclude={"workflow_id"}),
                db=db,
            )

    return db_workflow
