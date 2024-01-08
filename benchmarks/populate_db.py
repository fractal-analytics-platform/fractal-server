# import asyncio
import contextlib

from sqlmodel import select

from fractal_server.app.db import get_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.dataset import Dataset
from fractal_server.app.models.dataset import Resource
from fractal_server.app.models.project import Project
from fractal_server.app.schemas.dataset import DatasetCreate
from fractal_server.app.schemas.dataset import ResourceCreate
from fractal_server.app.schemas.project import ProjectCreate
from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.security import get_user_db
from fractal_server.app.security import get_user_manager
from fractal_server.app.security import User

# from fractal_server.app.models.job import ApplyWorkflow
# from fractal_server.app.models.workflow import Workflow
# from fractal_server.app.schemas.applyworkflow import ApplyWorkflowCreate
# from fractal_server.app.schemas.workflow import WorkflowCreate


get_async_session_context = contextlib.asynccontextmanager(get_db)
get_user_db_context = contextlib.asynccontextmanager(get_user_db)
get_user_manager_context = contextlib.asynccontextmanager(get_user_manager)


async def create_active_user(
    email: str,
    password: str,
    is_active: bool = True,
) -> None:

    async with get_async_session_context() as session:

        async with get_user_db_context(session) as user_db:
            async with get_user_manager_context(user_db) as user_manager:
                kwargs = dict(
                    email=email, password=password, is_active=is_active
                )
                user = await user_manager.create(UserCreate(**kwargs))
                print(f"User {user.email} created")


def populate_admin_user() -> None:

    with next(get_sync_db()) as db:

        stm = select(User).where(User.id == 1)
        res = db.execute(stm)
        user = res.scalars().first()

        if user is None:
            raise

        for i in range(0, 10):
            project_create = ProjectCreate(name=f"test_{i}_pro")
            project = Project(**project_create.dict())
            project.user_list.append(user)
            db.add(project)
            db.commit()
            db.refresh(project)

            dataset_create = DatasetCreate(name=f"test_{i}_ds", type="image")
            dataset = Dataset(project_id=project.id, **dataset_create.dict())
            db.add(dataset)
            db.commit()
            db.refresh(dataset)

            resource_create = ResourceCreate(path=f"/tmp{i}")  # nosec
            resource = Resource(
                dataset_id=dataset.id, **resource_create.dict()
            )
            db.add(resource)
            db.commit()


if __name__ == "__main__":

    populate_admin_user()

    # asyncio.run(create_active_user(email="active_user@fractal.xy",
    #                                password="5678"))
