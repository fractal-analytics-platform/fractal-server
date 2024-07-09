import logging
import random
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

from fractal_server.app.db import get_async_db
from fractal_server.app.security import _create_first_user
from fractal_server.config import get_settings
from fractal_server.config import Settings
from fractal_server.syringe import Inject
from tests.fixtures_slurm import HAS_LOCAL_SBATCH


try:
    import psycopg  # noqa: F401

    DB_ENGINE = "postgres-psycopg"

except ModuleNotFoundError:
    try:
        import psycopg2  # noqa: F401
        import asyncpg  # noqa: F401

        DB_ENGINE = "postgres"
    except ModuleNotFoundError:
        DB_ENGINE = "sqlite"


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
    elif DB_ENGINE in ["postgres", "postgres-psycopg"]:
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
    settings.FRACTAL_API_SUBMIT_RATE_LIMIT = 0
    settings.FRACTAL_API_MAX_JOB_LIST_LENGTH = 1
    settings.FRACTAL_GRACEFUL_SHUTDOWN_TIME = 1

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
    from sqlmodel import SQLModel

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
    app.state.jobsV1 = []
    app.state.jobsV2 = []
    app.state.connection = None
    app.state.fractal_ssh = None
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
        return f"{random.randint(0, 100000000)}@example.org"

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
