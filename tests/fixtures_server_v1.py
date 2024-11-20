import json
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.fixture
async def project_factory(db):
    """
    Factory that adds a project to the database
    """

    from fractal_server.app.models.v1 import Project

    async def __project_factory(user, **kwargs):
        defaults = dict(name="project")
        defaults.update(kwargs)
        project = Project(**defaults)
        project.user_list.append(user)
        db.add(project)
        await db.commit()
        await db.refresh(project)
        return project

    return __project_factory


@pytest.fixture
async def dataset_factory(db: AsyncSession):
    """
    Insert dataset in db
    """
    from fractal_server.app.models.v1 import Dataset
    from fractal_server.app.models.v1 import Project

    async def __dataset_factory(db: AsyncSession = db, **kwargs):
        defaults = dict(
            name="My Dataset",
            project_id=1,
        )
        args = dict(**defaults)
        args.update(kwargs)

        project_id = args["project_id"]
        project = await db.get(Project, project_id)
        if project is None:
            raise IndexError(
                "Error from dataset_factory: "
                f"Project {project_id} does not exist."
            )

        _dataset = Dataset(**args)
        db.add(_dataset)
        db.add(project)
        await db.commit()
        await db.refresh(_dataset)
        return _dataset

    return __dataset_factory


@pytest.fixture
async def resource_factory(db, testdata_path):
    from fractal_server.app.models.v1 import Dataset, Resource

    async def __resource_factory(dataset: Dataset, **kwargs):
        """
        Add a new resource to dataset
        """
        defaults = dict(path=(testdata_path / "png").as_posix())
        defaults.update(kwargs)
        resource = Resource(dataset_id=dataset.id, **defaults)
        db.add(resource)
        await db.commit()
        await db.refresh(dataset)
        return resource

    return __resource_factory


@pytest.fixture
async def task_factory(db: AsyncSession):
    """
    Insert task in db
    """
    from fractal_server.app.models.v1 import Task

    async def __task_factory(db: AsyncSession = db, index: int = 0, **kwargs):
        defaults = dict(
            name=f"task{index}",
            input_type="zarr",
            output_type="zarr",
            command="cmd",
            source="source",
        )
        args = dict(**defaults)
        args.update(kwargs)
        t = Task(**args)
        db.add(t)
        await db.commit()
        await db.refresh(t)
        return t

    return __task_factory


@pytest.fixture
async def job_factory(db: AsyncSession):
    """
    Insert job in db
    """

    from fractal_server.app.models.v1 import Dataset
    from fractal_server.app.models.v1 import Project
    from fractal_server.app.models.v1 import ApplyWorkflow
    from fractal_server.app.models.v1 import Workflow
    from fractal_server.app.runner.set_start_and_last_task_index import (
        set_start_and_last_task_index,
    )

    async def __job_factory(
        project_id: int,
        input_dataset_id: int,
        output_dataset_id: int,
        workflow_id: int,
        working_dir: Path,
        db: AsyncSession = db,
        **kwargs,
    ):
        workflow = await db.get(Workflow, workflow_id)
        if workflow is None:
            raise IndexError(
                "Error from job_factory: "
                f"Workflow {workflow_id} does not exist."
            )

        first_task_index, last_task_index = set_start_and_last_task_index(
            len(workflow.task_list),
            kwargs.get("first_task_index", None),
            kwargs.get("last_task_index", None),
        )

        input_dataset = await db.get(Dataset, input_dataset_id)
        if input_dataset is None:
            raise IndexError(
                "Error from job_factory: "
                f"Dataset {input_dataset_id} does not exist."
            )
        output_dataset = await db.get(Dataset, output_dataset_id)
        if output_dataset is None:
            raise IndexError(
                "Error from job_factory: "
                f"Dataset {input_dataset_id} does not exist."
            )
        project = await db.get(Project, project_id)
        if project is None:
            raise IndexError(
                "Error from job_factory: "
                f"Project {project_id} does not exist."
            )

        args = dict(
            project_id=project_id,
            input_dataset_id=input_dataset_id,
            output_dataset_id=output_dataset_id,
            workflow_id=workflow_id,
            input_dataset_dump=dict(
                **json.loads(
                    input_dataset.json(exclude={"resource_list", "history"})
                ),
                resource_list=[
                    resource.model_dump()
                    for resource in input_dataset.resource_list
                ],
            ),
            output_dataset_dump=dict(
                **json.loads(
                    output_dataset.json(exclude={"resource_list", "history"})
                ),
                resource_list=[
                    resource.model_dump()
                    for resource in output_dataset.resource_list
                ],
            ),
            workflow_dump=dict(
                **json.loads(workflow.json(exclude={"task_list"})),
                task_list=[
                    dict(
                        wf_task.model_dump(exclude={"task"}),
                        task=wf_task.task.model_dump(),
                    )
                    for wf_task in workflow.task_list
                ],
            ),
            project_dump=dict(
                **json.loads(project.json(exclude={"user_list"}))
            ),
            last_task_index=last_task_index,
            first_task_index=first_task_index,
            working_dir=working_dir,
            worker_init="WORKER_INIT string",
            user_email="user@example.org",
        )
        args.update(**kwargs)
        job = ApplyWorkflow(**args)
        db.add(job)
        db.add(project)
        await db.commit()
        await db.refresh(job)
        return job

    return __job_factory


@pytest.fixture
async def workflow_factory(db: AsyncSession):
    """
    Insert workflow in db
    """
    from fractal_server.app.models.v1 import Workflow
    from fractal_server.app.models.v1 import Project

    async def __workflow_factory(db: AsyncSession = db, **kwargs):
        defaults = dict(
            name="my workflow",
            project_id=1,
        )
        args = dict(**defaults)
        args.update(kwargs)

        project_id = args["project_id"]
        project = await db.get(Project, project_id)
        if project is None:
            raise IndexError(
                "Error from workflow_factory: "
                f"Project {project_id} does not exist."
            )

        w = Workflow(**args)
        db.add(w)
        db.add(project)
        await db.commit()
        await db.refresh(w)
        return w

    return __workflow_factory


@pytest.fixture
async def workflowtask_factory(db: AsyncSession):
    """
    Insert workflowtask in db
    """
    from fractal_server.app.models.v1 import WorkflowTask

    async def __workflowtask_factory(
        workflow_id: int, task_id: int, db: AsyncSession = db, **kwargs
    ):
        defaults = dict(
            workflow_id=workflow_id,
            task_id=task_id,
        )
        args = dict(**defaults)
        args.update(kwargs)
        wft = WorkflowTask(**args)
        db.add(wft)
        await db.commit()
        await db.refresh(wft)
        return wft

    return __workflowtask_factory
