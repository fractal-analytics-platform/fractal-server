"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import logging
import random
import shutil
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from typing import AsyncGenerator
from typing import Optional

import pytest
from asgi_lifespan import LifespanManager
from devtools import debug
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from fractal_server.app.db import get_async_db
from fractal_server.app.routes.api.v1.project import _encode_as_utc
from fractal_server.app.security import _create_first_user
from fractal_server.config import get_settings
from fractal_server.config import Settings
from fractal_server.syringe import Inject

try:
    import asyncpg  # noqa: F401

    DB_ENGINE = "postgres"
except ModuleNotFoundError:
    DB_ENGINE = "sqlite"

HAS_LOCAL_SBATCH = bool(shutil.which("sbatch"))


def check_python_has_venv(python_path: str, temp_path: Path):
    """
    This function checks that we can safely use a certain python interpreter,
    namely
    1. It exists;
    2. It has the venv module installed.
    """

    import subprocess
    import shlex

    temp_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path.parent.chmod(0o755)
    temp_path.mkdir(parents=True, exist_ok=True)
    temp_path.chmod(0o755)

    cmd = f"{python_path} -m venv {temp_path.as_posix()}"
    p = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
    )
    if p.returncode != 0:
        debug(cmd)
        debug(p.stdout.decode("UTF-8"))
        debug(p.stderr.decode("UTF-8"))
        logging.warning(
            "check_python_has_venv({python_path=}, {temp_path=}) failed."
        )
        raise RuntimeError(
            p.stderr.decode("UTF-8"),
            f"Hint: is the venv module installed for {python_path}? "
            f'Try running "{cmd}".',
        )


def get_patched_settings(temp_path: Path):
    settings = Settings()
    settings.JWT_SECRET_KEY = "secret_key"

    settings.FRACTAL_DEFAULT_ADMIN_USERNAME = "admin"

    settings.DB_ENGINE = DB_ENGINE
    if DB_ENGINE == "sqlite":
        settings.SQLITE_PATH = f"{temp_path.as_posix()}/_test.db"
    elif DB_ENGINE == "postgres":
        settings.DB_ENGINE = "postgres"
        settings.POSTGRES_USER = "postgres"
        settings.POSTGRES_PASSWORD = "postgres"
        settings.POSTGRES_DB = "fractal_test"
    else:
        raise ValueError

    settings.FRACTAL_TASKS_DIR = temp_path / "fractal_tasks_dir"
    settings.FRACTAL_TASKS_DIR.mkdir(parents=True, exist_ok=True)
    settings.FRACTAL_TASKS_DIR.chmod(0o755)
    settings.FRACTAL_RUNNER_WORKING_BASE_DIR = temp_path / "artifacts"
    settings.FRACTAL_RUNNER_WORKING_BASE_DIR.mkdir(parents=True, exist_ok=True)
    settings.FRACTAL_RUNNER_WORKING_BASE_DIR.chmod(0o755)
    settings.FRACTAL_API_SUBMIT_MIN_WAIT = 0

    # NOTE:
    # This variable is set to work with the system interpreter within a docker
    # container. If left unset it defaults to `sys.executable`
    if not HAS_LOCAL_SBATCH:
        settings.FRACTAL_SLURM_WORKER_PYTHON = "/usr/bin/python3"
        check_python_has_venv(
            "/usr/bin/python3", temp_path / "check_python_has_venv"
        )

    settings.FRACTAL_SLURM_CONFIG_FILE = temp_path / "slurm_config.json"

    settings.FRACTAL_SLURM_POLL_INTERVAL = 1
    settings.FRACTAL_SLURM_ERROR_HANDLING_INTERVAL = 1

    settings.FRACTAL_LOGGING_LEVEL = logging.DEBUG
    return settings


@pytest.fixture(scope="session", autouse=True)
def override_settings(tmp777_session_path):
    tmp_path = tmp777_session_path("server_folder")

    settings = get_patched_settings(tmp_path)

    def _get_settings():
        return settings

    Inject.override(get_settings, _get_settings)
    try:
        yield settings
    finally:
        Inject.pop(get_settings)


@pytest.fixture(scope="function")
def override_settings_factory():
    from fractal_server.config import Settings

    # NOTE: using a mutable variable so that we can modify it from within the
    # inner function
    get_settings_orig = []

    def _overrride_settings_factory(**kwargs):
        # NOTE: extract patched settings *before* popping out the patch!
        settings = Settings(**Inject(get_settings).dict())
        get_settings_orig.append(Inject.pop(get_settings))
        for k, v in kwargs.items():
            setattr(settings, k, v)

        def _get_settings():
            return settings

        Inject.override(get_settings, _get_settings)

    try:
        yield _overrride_settings_factory
    finally:
        if get_settings_orig:
            Inject.override(get_settings, get_settings_orig[0])


@pytest.fixture
async def db_create_tables(override_settings):
    from fractal_server.app.db import DB
    from fractal_server.app.models import SQLModel

    # Calling both set_sync_db and set_async_db guarantees that a new pair of
    # sync/async engines is created every time.
    # This is needed for our Postgresql-based CI, due to the presence of Enums.
    # See
    # https://github.com/fractal-analytics-platform/fractal-server/pull/934#issuecomment-1782842865
    # and
    # https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#prepared-statement-cache.
    DB.set_sync_db()
    DB.set_async_db()

    engine = DB.engine_sync()
    engine_async = DB.engine_async()
    metadata = SQLModel.metadata
    metadata.create_all(engine)

    yield

    metadata.drop_all(engine)
    engine.dispose()
    await engine_async.dispose()


@pytest.fixture
async def db(db_create_tables):

    async for session in get_async_db():
        yield session


@pytest.fixture
async def db_sync(db_create_tables):
    from fractal_server.app.db import get_sync_db

    for session in get_sync_db():
        yield session


@pytest.fixture
async def app(override_settings) -> AsyncGenerator[FastAPI, Any]:
    app = FastAPI()
    yield app


@pytest.fixture
async def register_routers(app, override_settings):
    from fractal_server.main import collect_routers

    collect_routers(app)


@pytest.fixture
async def client(
    app: FastAPI, register_routers, db
) -> AsyncGenerator[AsyncClient, Any]:
    async with AsyncClient(
        app=app, base_url="http://test"
    ) as client, LifespanManager(app):
        yield client


@pytest.fixture
async def registered_client(
    app: FastAPI, register_routers, db
) -> AsyncGenerator[AsyncClient, Any]:

    EMAIL = "test@test.com"
    PWD = "12345"
    await _create_first_user(email=EMAIL, password=PWD, is_superuser=False)

    async with AsyncClient(
        app=app, base_url="http://test"
    ) as client, LifespanManager(app):
        data_login = dict(
            username=EMAIL,
            password=PWD,
        )
        res = await client.post("auth/token/login/", data=data_login)
        token = res.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def registered_superuser_client(
    app: FastAPI, register_routers, db
) -> AsyncGenerator[AsyncClient, Any]:
    EMAIL = "some-admin@fractal.xy"
    PWD = "some-admin-password"
    await _create_first_user(email=EMAIL, password=PWD, is_superuser=True)
    async with AsyncClient(
        app=app, base_url="http://test"
    ) as client, LifespanManager(app):
        data_login = dict(username=EMAIL, password=PWD)
        res = await client.post("auth/token/login/", data=data_login)
        token = res.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def MockCurrentUser(app, db):
    from fractal_server.app.security import current_active_verified_user
    from fractal_server.app.security import current_active_user
    from fractal_server.app.security import current_active_superuser
    from fractal_server.app.security import User

    def _random_email():
        return f"{random.randint(0, 100000000)}@exact-lab.it"

    @dataclass
    class _MockCurrentUser:
        """
        Context managed user override
        """

        name: str = "User Name"
        user_kwargs: Optional[dict[str, Any]] = None
        email: Optional[str] = field(default_factory=_random_email)
        previous_dependencies: dict = field(default_factory=dict)

        async def __aenter__(self):

            # FIXME: if user_kwargs has an "id" key-value pair, then we should
            # try to `db.get(User, id)` (and create a new one if it does not
            # exist). This would allow to re-use the same user again, if it is
            # not deleted after closing this context manager.

            # Create new user
            defaults = dict(
                email=self.email,
                hashed_password="fake_hashed_password",
                slurm_user="test01",
            )
            if self.user_kwargs:
                defaults.update(self.user_kwargs)
            self.user = User(name=self.name, **defaults)

            try:
                db.add(self.user)
                await db.commit()
                await db.refresh(self.user)
            except IntegrityError:
                # Safety net, in case of non-unique email addresses
                await db.rollback()
                self.user.email = _random_email()
                db.add(self.user)
                await db.commit()
                await db.refresh(self.user)
            # Removing object from test db session, so that we can operate
            # on user from other sessions
            db.expunge(self.user)

            # Find out which dependencies should be overridden, and store their
            # pre-override value
            if self.user.is_active:
                self.previous_dependencies[
                    current_active_user
                ] = app.dependency_overrides.get(current_active_user, None)
            if self.user.is_active and self.user.is_superuser:
                self.previous_dependencies[
                    current_active_superuser
                ] = app.dependency_overrides.get(
                    current_active_superuser, None
                )
            if self.user.is_active and self.user.is_verified:
                self.previous_dependencies[
                    current_active_verified_user
                ] = app.dependency_overrides.get(
                    current_active_verified_user, None
                )

            # Override dependencies in the FastAPI app
            for dep in self.previous_dependencies.keys():
                app.dependency_overrides[dep] = lambda: self.user

            return self.user

        async def __aexit__(self, *args, **kwargs):
            # Reset overridden dependencies to the original ones
            for dep, previous_dep in self.previous_dependencies.items():
                if previous_dep is not None:
                    app.dependency_overrides[dep] = previous_dep

    return _MockCurrentUser


@pytest.fixture
async def project_factory(db):
    """
    Factory that adds a project to the database
    """

    from fractal_server.app.models import Project

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
    from fractal_server.app.models import Dataset
    from fractal_server.app.models import Project

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
    from fractal_server.app.models import Dataset, Resource

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
    from fractal_server.app.models import Task

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
    from fractal_server.app.models import Dataset
    from fractal_server.app.models import Project
    from fractal_server.app.models import ApplyWorkflow
    from fractal_server.app.models import Workflow
    from fractal_server.app.runner.common import set_start_and_last_task_index

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
                input_dataset.model_dump(
                    exclude={"resource_list", "timestamp_created"}
                ),
                timestamp_created=_encode_as_utc(
                    input_dataset.timestamp_created
                ),
                resource_list=[
                    resource.model_dump()
                    for resource in input_dataset.resource_list
                ],
            ),
            output_dataset_dump=dict(
                output_dataset.model_dump(
                    exclude={"resource_list", "timestamp_created"}
                ),
                timestamp_created=_encode_as_utc(
                    output_dataset.timestamp_created
                ),
                resource_list=[
                    resource.model_dump()
                    for resource in output_dataset.resource_list
                ],
            ),
            workflow_dump=dict(
                workflow.model_dump(
                    exclude={"task_list", "timestamp_created"}
                ),
                timestamp_created=_encode_as_utc(workflow.timestamp_created),
                task_list=[
                    dict(
                        wf_task.model_dump(exclude={"task"}),
                        task=wf_task.task.model_dump(),
                    )
                    for wf_task in workflow.task_list
                ],
            ),
            project_dump=dict(
                project.model_dump(exclude={"user_list", "timestamp_created"}),
                timestamp_created=_encode_as_utc(project.timestamp_created),
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
    from fractal_server.app.models import Workflow
    from fractal_server.app.models import Project

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
    from fractal_server.app.models import WorkflowTask

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
