import time
from collections.abc import AsyncGenerator
from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from devtools import debug
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.security import _create_first_user
from fractal_server.config import DataSettings
from fractal_server.config import EmailSettings
from fractal_server.config import get_data_settings
from fractal_server.config import get_email_settings
from fractal_server.config import get_oauth_settings
from fractal_server.config import get_settings
from fractal_server.config import OAuthSettings
from fractal_server.config import Settings
from fractal_server.syringe import Inject


PROJECT_DIR_PLACEHOLDER = "/fake/placeholder"


@pytest.fixture(scope="function")
def override_settings_factory():
    """
    Returns a function that can be used to override settings.
    """

    original_dependency = Inject._dependencies.get(get_settings, None)

    def _overrride_settings(**kwargs):
        # Create and validate new `Settings` object
        _original_settings = Inject(get_settings)
        _data = _original_settings.model_dump()
        _data.update(kwargs)
        _new_settings = Settings(**_data)

        def _patched_get_settings():
            return _new_settings

        Inject.override(get_settings, _patched_get_settings)

    try:
        yield _overrride_settings

    finally:
        # Restore initial configuration
        if original_dependency is None:
            if get_settings in Inject._dependencies.keys():
                Inject._dependencies.pop(get_settings)
        else:
            Inject._dependencies[get_settings] = original_dependency


@pytest.fixture(scope="function")
def override_email_settings_factory():
    """
    Returns a function that can be used to override email settings.
    """

    original_dependency = Inject._dependencies.get(get_email_settings, None)

    def _overrride_email_settings(**kwargs):
        # Create and validate new `Settings` object
        _original_settings = Inject(get_email_settings)
        _data = _original_settings.model_dump()
        _data.update(kwargs)
        _new_settings = EmailSettings(**_data)

        def _patched_get_email_settings():
            return _new_settings

        Inject.override(get_email_settings, _patched_get_email_settings)

    try:
        yield _overrride_email_settings

    finally:
        # Restore initial configuration
        if original_dependency is None:
            if get_email_settings in Inject._dependencies.keys():
                Inject._dependencies.pop(get_email_settings)
        else:
            Inject._dependencies[get_email_settings] = original_dependency


@pytest.fixture(scope="function")
def override_oauth_settings_factory():
    """
    Returns a function that can be used to override email settings.
    """

    original_dependency = Inject._dependencies.get(get_oauth_settings, None)

    def _overrride_oauth_settings(**kwargs):
        # Create and validate new `Settings` object
        _original_settings = Inject(get_oauth_settings)
        _data = _original_settings.model_dump()
        _data.update(kwargs)
        _new_settings = OAuthSettings(**_data)

        def _patched_get_oauth_settings():
            return _new_settings

        Inject.override(get_oauth_settings, _patched_get_oauth_settings)

    try:
        yield _overrride_oauth_settings

    finally:
        # Restore initial configuration
        if original_dependency is None:
            if get_oauth_settings in Inject._dependencies.keys():
                Inject._dependencies.pop(get_oauth_settings)
        else:
            Inject._dependencies[get_oauth_settings] = original_dependency


@pytest.fixture(scope="function")
def override_data_settings_factory():
    """
    Returns a function that can be used to override email settings.
    """

    original_dependency = Inject._dependencies.get(get_data_settings, None)

    def _overrride_data_settings(**kwargs):
        # Create and validate new `Settings` object
        _original_settings = Inject(get_data_settings)
        _data = _original_settings.model_dump()
        _data.update(kwargs)
        _new_settings = DataSettings(**_data)

        # Override `get_settings`
        def _patched_get_data_settings():
            return _new_settings

        Inject.override(get_data_settings, _patched_get_data_settings)

    try:
        yield _overrride_data_settings

    finally:
        # Restore initial configuration
        if original_dependency is None:
            if get_oauth_settings in Inject._dependencies.keys():
                Inject._dependencies.pop(get_data_settings)
        else:
            Inject._dependencies[get_data_settings] = original_dependency


@pytest.fixture(scope="function")
async def db_create_tables():
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
    from fractal_server.app.db import get_async_db

    async for session in get_async_db():
        yield session


@pytest.fixture
async def db_sync(db_create_tables):
    from fractal_server.app.db import get_sync_db

    for session in get_sync_db():
        yield session


@pytest.fixture
def app() -> Generator[FastAPI, Any]:
    app = FastAPI()
    app.state.jobsV2 = []
    app.state.fractal_ssh_list = None
    yield app


@pytest.fixture
def register_routers(app):
    from fractal_server.main import collect_routers

    collect_routers(app)


@pytest.fixture
async def client(
    app: FastAPI, register_routers, db
) -> AsyncGenerator[AsyncClient, Any]:
    async with (
        AsyncClient(
            base_url="http://test", transport=ASGITransport(app=app)
        ) as client,
        LifespanManager(app),
    ):
        yield client


@pytest.fixture
async def registered_client(  # FIXME maybe remove?
    app: FastAPI, register_routers, db
) -> AsyncGenerator[AsyncClient, Any]:
    EMAIL = "test@test.com"
    PWD = "12345"
    await _create_first_user(
        email=EMAIL,
        password=PWD,
        is_superuser=False,
        project_dir=PROJECT_DIR_PLACEHOLDER,
    )

    async with (
        AsyncClient(
            base_url="http://test", transport=ASGITransport(app=app)
        ) as client,
        LifespanManager(app),
    ):
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
    app: FastAPI, register_routers, db, local_resource_profile_db
) -> AsyncGenerator[AsyncClient, Any]:
    EMAIL = "some-admin@example.org"
    PWD = "some-admin-password"
    resouce, profile = local_resource_profile_db
    await _create_first_user(
        email=EMAIL,
        password=PWD,
        is_superuser=True,
        project_dir=PROJECT_DIR_PLACEHOLDER,
        profile_id=profile.id,
    )
    async with (
        AsyncClient(
            base_url="http://test", transport=ASGITransport(app=app)
        ) as client,
        LifespanManager(app),
    ):
        data_login = dict(username=EMAIL, password=PWD)
        res = await client.post("auth/token/login/", data=data_login)
        token = res.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        yield client


@pytest.fixture
async def default_user_group(
    db: AsyncSession, override_settings_factory
) -> UserGroup | None:
    """
    Note: using this fixture also sets `FRACTAL_DEFAULT_GROUP_NAME="All"`
    in settings.
    """
    override_data_settings_factory(FRACTAL_DEFAULT_GROUP_NAME="All")
    _FRACTAL_DEFAULT_GROUP_NAME = "All"
    stm = select(UserGroup).where(
        UserGroup.name == _FRACTAL_DEFAULT_GROUP_NAME
    )
    res = await db.execute(stm)
    default_user_group = res.scalars().one_or_none()
    if default_user_group is None:
        default_user_group = UserGroup(name=_FRACTAL_DEFAULT_GROUP_NAME)
        db.add(default_user_group)
        await db.commit()
        await db.refresh(default_user_group)
    return default_user_group


@pytest.fixture
async def MockCurrentUser(
    app: FastAPI, db, default_user_group: UserGroup | None
):
    from fractal_server.app.routes.auth import (
        current_user_act_ver_prof,
        current_user_act,
        current_user_act_ver,
        current_superuser_act,
    )

    def _new_mail():
        return f"{time.perf_counter_ns()}@example.org"

    @dataclass
    class _MockCurrentUser:
        """
        Context managed user override
        """

        user_kwargs: dict[str, Any] = field(default_factory=dict)
        email: str | None = field(default_factory=_new_mail)
        previous_deps: dict = field(default_factory=dict)
        debug: bool = False

        async def __aenter__(self):
            user_id = self.user_kwargs.get("id", None)
            if user_id is not None:
                # (1) Look for existing user, by ID
                db_user = await db.get(
                    UserOAuth,
                    user_id,
                    populate_existing=True,
                )
                if self.debug:
                    debug("FOUND USER", db_user)
                if db_user is None:
                    raise RuntimeError(
                        f"[MockCurrentUser] User with {user_id=} doesn't exist"
                    )
                for k, v in self.user_kwargs.items():
                    if not getattr(db_user, k) == v:
                        raise RuntimeError(
                            f"[MockCurrentUser] User with {user_id=} has "
                            f"{k}={v}."
                        )
                self.user = db_user
            else:
                # (2) Create new user
                default_user_kwargs = dict(
                    email=self.email,
                    hashed_password="fake_hashed_password",
                    project_dir=PROJECT_DIR_PLACEHOLDER,
                    is_verified=True,
                )

                # (2/a) Handle resource and profile
                if "profile_id" not in self.user_kwargs.keys():
                    res = await db.execute(select(Profile))
                    profile = res.scalars().first()
                    if profile is None:
                        resource = Resource(
                            name="Local resource",
                            type=ResourceType.LOCAL,
                            jobs_local_dir="/jobs_local_dir",
                            tasks_local_dir="/tasks_local_dir",
                            jobs_runner_config={"parallel_tasks_per_job": 1},
                            tasks_python_config={
                                "default_version": "3.0",
                                "versions": {"3.0": "/fake/python3.0"},
                            },
                            tasks_pixi_config={},
                            jobs_poll_interval=0,
                        )
                        db.add(resource)
                        await db.commit()
                        await db.refresh(resource)
                        db.expunge(resource)
                        profile = Profile(
                            username="fake-username",
                            resource_id=resource.id,
                            name="local_resource_profile_objects",
                            resource_type=ResourceType.LOCAL,
                        )
                        db.add(profile)
                        await db.commit()
                        await db.refresh(profile)
                        db.expunge(profile)
                    default_user_kwargs["profile_id"] = profile.id

                # Create new user
                default_user_kwargs.update(self.user_kwargs)
                self.user = UserOAuth(**default_user_kwargs)
                db.add(self.user)
                await db.commit()
                await db.refresh(self.user)

                if self.debug:
                    debug("CREATED USER", self.user)
                if default_user_group is not None:
                    db.add(
                        LinkUserGroup(
                            user_id=self.user.id,
                            group_id=default_user_group.id,
                        )
                    )
                    await db.commit()
                    if self.debug:
                        debug(
                            f"Created link between user_id={self.user.id} and "
                            f"group_id={default_user_group.id}."
                        )

            # Removing objects from test db session, so that we can operate
            # on them from other sessions
            db.expunge(self.user)

            # Find out which dependencies should be overridden, and store their
            # pre-override value
            if self.user.is_active:
                dep = current_user_act
                self.previous_deps[dep] = app.dependency_overrides.get(
                    dep, None
                )
                if self.debug:
                    debug(f"Override {current_user_act}.")

            if self.user.is_active and self.user.is_verified:
                dep = current_user_act_ver
                self.previous_deps[dep] = app.dependency_overrides.get(
                    dep, None
                )
                if self.debug:
                    debug(f"Override {current_user_act_ver}.")

            if (
                self.user.is_active
                and self.user.is_verified
                and self.user.profile_id is not None
            ):
                dep = current_user_act_ver_prof
                if self.debug:
                    debug(f"Override {current_user_act_ver_prof}.")
                self.previous_deps[dep] = app.dependency_overrides.get(
                    dep, None
                )

            if self.user.is_active and self.user.is_superuser:
                dep = current_superuser_act
                if self.debug:
                    debug(f"Override {current_superuser_act}.")
                self.previous_deps[dep] = app.dependency_overrides.get(
                    dep, None
                )

            # Override dependencies in the FastAPI app
            for _dep in self.previous_deps.keys():
                app.dependency_overrides[_dep] = lambda: self.user

            return self.user

        async def __aexit__(self, *args, **kwargs):
            # Reset overridden dependencies to the original ones
            for _dep, previous_dep in self.previous_deps.items():
                if previous_dep is not None:
                    app.dependency_overrides[_dep] = previous_dep

    return _MockCurrentUser


@pytest.fixture(scope="function")
async def first_user(
    db: AsyncSession,
    default_user_group: UserGroup | None,
    local_resource_profile_db,
):
    """
    Make sure that at least one user exists.
    """
    resource, profile = local_resource_profile_db
    res = await db.execute(select(UserOAuth).order_by(UserOAuth.id))
    user = res.scalars().first()
    if user is None:
        user = UserOAuth(
            email="example@example.org",
            hashed_password="fake_password",
            is_active=True,
            is_verified=True,
            profile_id=profile.id,
        )
        db.add(user)
        await db.commit()
        db.expunge(user)

        if default_user_group is not None:
            db.add(
                LinkUserGroup(user_id=user.id, group_id=default_user_group.id)
            )
            await db.commit()

    return user


@pytest.fixture
async def user_group_factory(db: AsyncSession):
    """
    Insert UserGroup in db and link it to a UserOAuth
    """

    async def __user_group_factory(
        group_name: str,
        user_id: int,
        *other_users_id: list[int],
        db: AsyncSession = db,
    ):
        user_group = UserGroup(name=group_name)
        db.add(user_group)
        await db.commit()
        await db.refresh(user_group)

        db.add(LinkUserGroup(user_id=user_id, group_id=user_group.id))
        for other_user_id in other_users_id:
            db.add(
                LinkUserGroup(user_id=other_user_id, group_id=user_group.id)
            )

        await db.commit()

        return user_group

    return __user_group_factory
