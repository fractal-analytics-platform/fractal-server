import logging
from pathlib import Path

import pytest
from devtools import debug  # noqa

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_functions_task_collection import (
    get_package_version_from_pypi,
)
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


PREFIX = "api/v2/task"


@pytest.mark.parametrize("use_current_python", [True, False])
async def test_task_collection_from_wheel(
    db,
    client,
    MockCurrentUser,
    use_current_python: bool,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
    current_py_version: str,
):
    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    FRACTAL_MAX_PIP_VERSION = "24.0"
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=current_py_version,
        FRACTAL_MAX_PIP_VERSION=FRACTAL_MAX_PIP_VERSION,
    )

    # Prepare absolute path to wheel file
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    payload_package = wheel_path.as_posix()

    # Prepare and validate payload
    payload = dict(package=payload_package, package_extras="my_extra")
    debug(payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        assert res.json()["log"] is None
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        assert res.status_code == 200
        task_group_activity = res.json()
        assert task_group_activity["status"] == "OK"
        # Check that log were written, even with CRITICAL logging level
        log = task_group_activity["log"]
        assert log is not None
        # Check that my_extra was included, in a local-package collection
        assert ".whl[my_extra]" in log

        # A second identical collection fails
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        assert res.status_code == 422


async def test_task_collection_from_wheel_non_canonical(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
    current_py_version: str,
):
    """
    Same as test_task_collection_from_wheel, but package has a
    non-canonical name.
    """

    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=current_py_version,
    )

    # Prepare absolute path to wheel file
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_non_canonical/dist"
        / "FrAcTaL_TaSkS_NoN_CaNoNiCaL-0.0.1-py3-none-any.whl"
    )
    payload_package = wheel_path.as_posix()

    # Prepare and validate payload
    payload = dict(package=payload_package, package_extras="my_extra")
    payload["python_version"] = current_py_version
    debug(payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        task_group_activity = res.json()
        # Get collection info
        assert res.status_code == 200

        # Check that log were written, even with CRITICAL logging level
        log = task_group_activity["log"]
        assert log is not None
        # Check that my_extra was included, in a local-package collection
        assert ".whl[my_extra]" in log
        assert task_group_activity["status"] == "OK"


OLD_FRACTAL_TASKS_CORE_VERSION = "1.0.2"


@pytest.mark.parametrize(
    "package_version", [None, OLD_FRACTAL_TASKS_CORE_VERSION]
)
async def test_task_collection_from_pypi(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    current_py_version,
    package_version,
):

    if (
        current_py_version == "3.12"
        and package_version == OLD_FRACTAL_TASKS_CORE_VERSION
    ):
        logging.warning(
            f"SKIP test_task_collection_from_pypi with {current_py_version=}. "
            "This is because fractal-tasks-core has a single version (1.3.2) "
            "which works with python3.12 (due to pandas required version). "
            "This means we cannot test the install of an old version like "
            "1.0.2."
        )
        return

    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=current_py_version,
    )
    settings = Inject(get_settings)

    # Prepare and validate payload
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    payload = dict(
        package="fractal-tasks-core",
        python_version=PYTHON_VERSION,
    )
    if package_version is None:
        EXPECTED_PACKAGE_VERSION = await get_package_version_from_pypi(
            payload["package"]
        )
        assert EXPECTED_PACKAGE_VERSION > OLD_FRACTAL_TASKS_CORE_VERSION
    else:
        EXPECTED_PACKAGE_VERSION = package_version
        payload["package_version"] = package_version

    debug(payload)
    debug(EXPECTED_PACKAGE_VERSION)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"

        # Get collection info
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        task_group_activity = res.json()
        assert task_group_activity["status"] == "OK"
        # Check that log were written, even with CRITICAL logging level
        log = task_group_activity["log"]
        assert log is not None

        # Collect again and fail due to non-duplication constraint
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        assert res.status_code == 422
        assert "already owns a task group" in res.json()["detail"]


async def test_task_collection_failure_due_to_existing_path(
    db, client, MockCurrentUser
):
    settings = Inject(get_settings)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        path = (
            settings.FRACTAL_TASKS_DIR / f"{user.id}/fractal-tasks-core/1.2.0/"
        ).as_posix()
        venv_path = (
            settings.FRACTAL_TASKS_DIR
            / f"{user.id}/fractal-tasks-core/1.2.0/venv/"
        ).as_posix()

        # Create fake task group
        tg = TaskGroupV2(
            origin="other",
            path=path,
            venv_path=venv_path,
            pkg_name="fractal-tasks-core-FAKE",
            version="1.2.0",
            user_id=user.id,
        )
        db.add(tg)
        await db.commit()
        await db.refresh(tg)
        db.expunge(tg)
        await db.close()

        # Collect again and fail due to another group having the same path set
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package="fractal-tasks-core", package_version="1.2.0"),
        )
        assert res.status_code == 422
        assert "Another task-group already has path" in res.json()["detail"]


async def test_contact_an_admin_message(
    MockCurrentUser, client, db, default_user_group
):
    # Create identical multiple (> 1) TaskGroups associated to userA and to the
    # default UserGroup (this is NOT ALLOWED using the API).
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as userA:
        for _ in range(2):
            db.add(
                TaskGroupV2(
                    user_id=userA.id,
                    user_group_id=default_user_group.id,
                    pkg_name="fractal-tasks-core",
                    version="1.0.0",
                    origin="pypi",
                )
            )
        await db.commit()

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as userB:

        # Fail inside `_verify_non_duplication_group_constraint`.
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package="fractal-tasks-core", package_version="1.0.0"),
        )
        assert res.status_code == 422
        assert "UserGroup " in res.json()["detail"]
        assert "contact an admin" in res.json()["detail"]

        # Create identical multiple (> 1) TaskGroups associated to userB
        # (this is NOT ALLOWED using the API).
        for _ in range(2):
            db.add(
                TaskGroupV2(
                    user_id=userB.id,
                    user_group_id=None,
                    pkg_name="fractal-tasks-core",
                    version="1.0.0",
                    origin="pypi",
                )
            )
        await db.commit()

        # Fail inside `_verify_non_duplication_user_constraint`.
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package="fractal-tasks-core", package_version="1.0.0"),
        )
        assert res.status_code == 422
        assert "User " in res.json()["detail"]
        assert "contact an admin" in res.json()["detail"]

        # Create a new TaskGroupV2 associated to userB.
        task_group = TaskGroupV2(
            user_id=userB.id,
            pkg_name="fractal-tasks-core",
            version="1.1.0",
            origin="pypi",
        )
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        # Create a TaskGroupActivityStatusV2 associated to the new TaskGroup.
        db.add(
            TaskGroupActivityV2(
                user_id=userB.id,
                taskgroup_id=task_group.id,
                action=TaskGroupActivityActionV2.COLLECT,
                status=TaskGroupActivityStatusV2.PENDING,
                pkg_name="fractal-tasks-core",
                version="1.1.0",
            )
        )
        await db.commit()

        # Fail inside `_verify_non_duplication_user_constraint`,
        # (case `len(states) == 1`).
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package="fractal-tasks-core", package_version="1.1.0"),
        )
        assert res.status_code == 422
        assert "There exists a task-group activity" in res.json()["detail"]
