from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2.submit import _encode_as_utc
from fractal_server.app.runner.set_start_and_last_task_index import (
    set_start_and_last_task_index,
)


@pytest.fixture
async def project_factory_v2(db):
    """
    Factory that adds a ProjectV2 to the database
    """

    async def __project_factory(user, **kwargs):
        defaults = dict(name="project")
        defaults.update(kwargs)
        project = ProjectV2(**defaults)
        project.user_list.append(user)
        db.add(project)
        await db.commit()
        await db.refresh(project)
        return project

    return __project_factory


@pytest.fixture
async def dataset_factory_v2(db: AsyncSession, tmp_path):
    """
    Insert DatasetV2 in db
    """

    async def __dataset_factory_v2(db: AsyncSession = db, **kwargs):
        defaults = dict(
            name="My Dataset", project_id=1, zarr_dir=f"{tmp_path}/zarr"
        )
        args = dict(**defaults)
        args.update(kwargs)

        project_id = args["project_id"]
        project = await db.get(ProjectV2, project_id)
        if project is None:
            raise IndexError(
                "Error from dataset_factory: "
                f"Project {project_id} does not exist."
            )

        _dataset = DatasetV2(**args)
        db.add(_dataset)
        await db.commit()
        await db.refresh(_dataset)
        return _dataset

    return __dataset_factory_v2


@pytest.fixture
async def workflow_factory_v2(db: AsyncSession):
    """
    Insert WorkflowV2 in db
    """

    async def __workflow_factory(db: AsyncSession = db, **kwargs):
        defaults = dict(
            name="My Workflow",
            project_id=1,
        )
        args = dict(**defaults)
        args.update(kwargs)

        project_id = args["project_id"]
        project = await db.get(ProjectV2, project_id)
        if project is None:
            raise IndexError(
                "Error from workflow_factory: "
                f"Project {project_id} does not exist."
            )

        w = WorkflowV2(**args)
        db.add(w)
        await db.commit()
        await db.refresh(w)
        return w

    return __workflow_factory


@pytest.fixture
async def job_factory_v2(db: AsyncSession):
    """
    Insert JobV2 in db
    """

    async def __job_factory(
        project_id: int,
        dataset_id: int,
        workflow_id: int,
        working_dir: Path,
        db: AsyncSession = db,
        **kwargs,
    ):
        workflow = await db.get(WorkflowV2, workflow_id)
        if workflow is None:
            raise IndexError(
                "Error from job_factory: "
                f"WorkflowV2 {workflow_id} does not exist."
            )

        first_task_index, last_task_index = set_start_and_last_task_index(
            len(workflow.task_list),
            kwargs.get("first_task_index", None),
            kwargs.get("last_task_index", None),
        )

        dataset = await db.get(DatasetV2, dataset_id)
        if dataset is None:
            raise IndexError(
                "Error from job_factory: "
                f"DatasetV2 {dataset_id} does not exist."
            )

        project = await db.get(ProjectV2, project_id)
        if project is None:
            raise IndexError(
                "Error from job_factory: "
                f"ProjectV2 {project_id} does not exist."
            )
        _working_dir = (
            working_dir.as_posix()
            if isinstance(working_dir, Path)
            else working_dir
        )
        args = dict(
            project_id=project_id,
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            dataset_dump=dict(
                dataset.model_dump(
                    exclude={"timestamp_created", "history", "images"}
                ),
                timestamp_created=_encode_as_utc(dataset.timestamp_created),
            ),
            workflow_dump=dict(
                workflow.model_dump(
                    exclude={"task_list", "timestamp_created"}
                ),
                timestamp_created=_encode_as_utc(workflow.timestamp_created),
            ),
            project_dump=dict(
                project.model_dump(exclude={"user_list", "timestamp_created"}),
                timestamp_created=_encode_as_utc(project.timestamp_created),
            ),
            last_task_index=last_task_index,
            first_task_index=first_task_index,
            working_dir=_working_dir,
            worker_init="WORKER_INIT string",
            user_email="user@example.org",
        )
        args.update(**kwargs)
        job = JobV2(**args)
        db.add(job)
        await db.commit()
        await db.refresh(job)
        return job

    return __job_factory


@pytest.fixture
async def task_factory_v2(db: AsyncSession):
    """
    Insert TaskV2 in db
    """

    async def __task_factory(
        db: AsyncSession = db,
        index: int = 0,
        type: Literal["parallel", "non_parallel", "compound"] = "compound",
        **kwargs,
    ):
        args = dict(
            type=type,
            name=f"task{index}",
            source=f"source{index}",
            version=f"{index}",
            command_parallel="cmd_parallel",
            command_non_parallel="cmd_non_parallel",
        )
        if type == "parallel":
            if any(
                arg in kwargs
                for arg in [
                    "meta_non_parallel",
                    "args_schema_non_parallel",
                    "command_non_parallel",
                ]
            ):
                raise TypeError("Invalid argument for a parallel TaskV2")
            else:
                del args["command_non_parallel"]
        elif type == "non_parallel":
            if any(
                arg in kwargs
                for arg in [
                    "meta_parallel",
                    "args_schema_parallel",
                    "command_parallel",
                ]
            ):
                raise TypeError("Invalid argument for a non_parallel TaskV2")
            else:
                del args["command_parallel"]

        args.update(kwargs)
        t = TaskV2(**args)
        db.add(t)
        await db.commit()
        await db.refresh(t)
        return t

    return __task_factory


@pytest.fixture
async def workflowtask_factory_v2(db: AsyncSession):
    """
    Insert workflowtaskv2 in db
    """

    async def __workflowtask_factory(
        workflow_id: int, task_id: int, db: AsyncSession = db, **kwargs
    ):
        task = await db.get(TaskV2, task_id)
        if task is None:
            raise Exception(f"TaskV2[{task_id}] not found.")
        defaults = dict(
            workflow_id=workflow_id,
            task_id=task_id,
            is_legacy_task=False,
            task_type=task.type,
        )
        args = dict(**defaults)
        args.update(kwargs)
        wft = WorkflowTaskV2(**args)
        db.add(wft)
        await db.commit()
        await db.refresh(wft)
        return wft

    return __workflowtask_factory
