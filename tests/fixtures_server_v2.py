import json
from typing import Literal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _verify_non_duplication_group_constraint,
)  # noqa
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _verify_non_duplication_user_constraint,
)  # noqa
from fractal_server.app.routes.auth._aux_auth import (
    _get_default_usergroup_id_or_none,
)
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.images.models import SingleImage
from fractal_server.runner.set_start_and_last_task_index import (
    set_start_and_last_task_index,
)
from fractal_server.urls import normalize_url


@pytest.fixture
async def project_factory_v2(db):
    """
    Factory that adds a ProjectV2 to the database
    """

    async def __project_factory(user, **kwargs):
        resource_id = kwargs.get("resource_id", None)
        if resource_id is None:
            res = await db.execute(
                select(Resource.id)
                .join(Profile)
                .where(Resource.id == Profile.resource_id)
                .where(Profile.id == user.profile_id)
            )
            resource_id = res.scalar_one()
        args = dict(
            name="project",
            resource_id=resource_id,
            project_dir="/fake",
        )
        args.update(kwargs)
        project = ProjectV2(**args)
        db.add(project)
        await db.flush()

        link = LinkUserProjectV2(project_id=project.id, user_id=user.id)
        db.add(link)

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
            name="My Dataset",
            project_id=1,
            zarr_dir=f"{tmp_path}/zarr",
        )
        args = dict(**defaults)
        args.update(kwargs)

        # Make sure that `zarr_dir` and images are valid
        args["zarr_dir"] = normalize_url(args["zarr_dir"])
        old_images = args.get("images", [])
        args["images"] = [
            SingleImage(**img).model_dump() for img in old_images
        ]

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
        working_dir: str,
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
        args = dict(
            project_id=project_id,
            dataset_id=dataset_id,
            workflow_id=workflow_id,
            dataset_dump=json.loads(
                dataset.model_dump_json(exclude={"history", "images"})
            ),
            workflow_dump=json.loads(
                workflow.model_dump_json(exclude={"task_list"})
            ),
            project_dump=json.loads(
                project.model_dump_json(exclude={"resource_id"})
            ),
            last_task_index=last_task_index,
            first_task_index=first_task_index,
            working_dir=working_dir,
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
        user_id: int,
        task_group_kwargs: dict[str, str] | None = None,
        db: AsyncSession = db,
        index: int = 0,
        type: Literal[
            "parallel",
            "non_parallel",
            "compound",
            "converter_compound",
            "converter_non_parallel",
        ] = "compound",
        **kwargs,
    ) -> TaskV2:
        args = dict(
            type=type,
            name=f"task{index}",
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
        elif type == "non_parallel" or type == "converter_non_parallel":
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
        task = TaskV2(**args)

        # Task Group
        if task_group_kwargs is None:
            task_group_kwargs = dict()

        if "user_group_id" not in task_group_kwargs.keys():
            user_group_id = await _get_default_usergroup_id_or_none(db=db)
        else:
            user_group_id = task_group_kwargs["user_group_id"]
            if user_group_id is not None:
                await _verify_user_belongs_to_group(
                    user_id=user_id, user_group_id=user_group_id, db=db
                )

        pkg_name = task_group_kwargs.get("pkg_name", task.name)
        version = task_group_kwargs.get("version", task.version)
        res = await db.execute(
            select(Resource.id)
            .join(Profile)
            .join(UserOAuth)
            .where(Resource.id == Profile.resource_id)
            .where(Profile.id == UserOAuth.profile_id)
            .where(UserOAuth.id == user_id)
        )
        resource_id = res.scalar_one()

        await _verify_non_duplication_user_constraint(
            db=db,
            user_id=user_id,
            pkg_name=pkg_name,
            version=version,
            user_resource_id=resource_id,
        )
        await _verify_non_duplication_group_constraint(
            db=db,
            user_group_id=user_group_id,
            pkg_name=pkg_name,
            version=version,
        )

        task_group = TaskGroupV2(
            user_id=user_id,
            user_group_id=user_group_id,
            resource_id=resource_id,
            active=task_group_kwargs.get("active", True),
            version=version,
            origin=task_group_kwargs.get("origin", "other"),
            pkg_name=pkg_name,
            path=task_group_kwargs.get("path", None),
            venv_path=task_group_kwargs.get("venv_path", None),
            python_version=task_group_kwargs.get("python_version", None),
            task_list=[task],
        )
        db.add(task_group)
        await db.commit()

        await db.refresh(task)
        return task

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
        workflow = await db.get(WorkflowV2, workflow_id)
        if workflow is None:
            raise Exception(f"WorkflowV2[{workflow_id}] not found.")

        defaults = dict(
            workflow_id=workflow_id,
            task_id=task_id,
            task_type=task.type,
        )
        args = dict(**defaults)
        args.update(kwargs)
        wft = WorkflowTaskV2(**args)

        if wft.order is not None:
            order = wft.order
        else:
            order = len(workflow.task_list)

        workflow.task_list.insert(order, wft)
        flag_modified(workflow, "task_list")
        await db.commit()
        await db.refresh(wft)
        return wft

    return __workflowtask_factory


@pytest.fixture
async def valid_user_id(db: AsyncSession) -> int:
    user = UserOAuth(
        email="fake@example.org",
        hashed_password="fake-hashed-password",
        project_dir="/fake",
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user.id
