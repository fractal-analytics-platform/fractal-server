import json
import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_functions_task_lifecycle import (
    get_package_version_from_pypi,
)

PREFIX = "api/v2/task"


async def test_task_collection_from_wheel_non_canonical(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
    current_py_version: str,
    local_resource_profile_db,
):
    """
    Same as test_task_collection_from_wheel, but package has a
    non-canonical name.
    """

    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(FRACTAL_LOGGING_LEVEL=logging.CRITICAL)

    resource, profile = local_resource_profile_db

    # Prepare absolute path to wheel file
    archive_path = (
        testdata_path.parent
        / "v2/fractal_tasks_non_canonical/dist"
        / "FrAcTaL_TaSkS_NoN_CaNoNiCaL-0.0.1-py3-none-any.whl"
    )
    with open(archive_path, "rb") as f:
        files = {"file": (archive_path.name, f.read(), "application/zip")}

    # Prepare and validate payload
    payload = dict(package_extras="my_extra")
    payload["python_version"] = current_py_version
    debug(payload)

    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/", data=payload, files=files
        )
        debug(res.json())
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
        debug(log)
        assert ".whl[my_extra]" in log
        assert task_group_activity["status"] == "OK"
        assert task_group_activity["timestamp_ended"] is not None


OLD_FAKE_PACKAGE_VERSION = "0.0.3"


@pytest.mark.parametrize("package_version", [None, OLD_FAKE_PACKAGE_VERSION])
async def test_task_collection_from_pypi_api_only(
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    current_py_version,
    package_version,
    monkeypatch,
    local_resource_profile_db,
):
    import fractal_server.app.routes.api.v2.task_collection  # noqa

    def fake_collect_local(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(
        "fractal_server.app.routes.api.v2.task_collection.collect_local",
        fake_collect_local,
    )

    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(FRACTAL_LOGGING_LEVEL=logging.CRITICAL)

    # Prepare and validate payload
    payload = dict(
        package="testing-tasks-mock",
        python_version=current_py_version,
    )
    if package_version is None:
        EXPECTED_PACKAGE_VERSION = await get_package_version_from_pypi(
            payload["package"]
        )
        assert EXPECTED_PACKAGE_VERSION > OLD_FAKE_PACKAGE_VERSION
    else:
        EXPECTED_PACKAGE_VERSION = package_version
        payload["package_version"] = package_version

    debug(payload)
    debug(EXPECTED_PACKAGE_VERSION)
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/?private=true",
            data=payload,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"

        # Get collection info
        task_group_activity_id = res.json()["id"]
        res = await client.get(
            f"/api/v2/task-group/activity/{task_group_activity_id}/"
        )
        task_group_activity = res.json()
        debug(task_group_activity)
        assert task_group_activity["version"] == EXPECTED_PACKAGE_VERSION


async def test_task_collection_from_pypi(
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    current_py_version,
    local_resource_profile_db,
):
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(FRACTAL_LOGGING_LEVEL=logging.CRITICAL)

    # Prepare and validate payload
    payload = dict(
        package="testing-tasks-mock",
        python_version=current_py_version,
    )
    EXPECTED_PACKAGE_VERSION = await get_package_version_from_pypi(
        payload["package"]
    )
    assert EXPECTED_PACKAGE_VERSION > OLD_FAKE_PACKAGE_VERSION

    debug(payload)
    debug(EXPECTED_PACKAGE_VERSION)

    resource, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/?private=true",
            data=payload,
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
        assert task_group_activity["timestamp_ended"] is not None
        assert task_group_activity["log"] is not None
        debug(task_group_activity["log"])

        # Get task-group info
        task_group_id = res.json()["taskgroupv2_id"]
        res = await client.get(f"/api/v2/task-group/{task_group_id}/")
        task_group = res.json()
        assert task_group["user_group_id"] is None

        res = await client.post(f"{PREFIX}/collect/pip/", data=payload)
        assert res.status_code == 422
        assert "already exists." in res.json()["detail"]


async def test_task_collection_failure_due_to_existing_path(
    db, client, MockCurrentUser, local_resource_profile_db
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        path = (
            Path(resource.tasks_local_dir)
            / f"{user.id}/testing-tasks-mock/0.1.4/"
        ).as_posix()
        venv_path = (
            Path(resource.tasks_local_dir)
            / f"{user.id}/testing-tasks-mock/0.1.4/venv/"
        ).as_posix()

        # Create fake task group
        tg = TaskGroupV2(
            origin="other",
            path=path,
            venv_path=venv_path,
            pkg_name="testing-tasks-mock-FAKE",
            version="0.1.4",
            user_id=user.id,
            resource_id=resource.id,
        )
        db.add(tg)
        await db.commit()
        await db.refresh(tg)
        db.expunge(tg)
        await db.close()

        # Collect again and fail due to another group having the same path set
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(package="testing-tasks-mock", package_version="0.1.4"),
        )
        assert res.status_code == 422
        assert "Other TaskGroups already have path" in res.json()["detail"]


async def test_task_collection_from_pypi_with_extras(
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    ERROR_MESSAGE = "Command must not contain any of this characters"
    _, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core",
                package_version="99.99.99",
                pinned_package_versions_pre=json.dumps(
                    {"fastapi[standard]": "99.99.99"}
                ),
                pinned_package_versions_post=json.dumps(
                    {"uvicorn[standard]": "99.99.99"}
                ),
            ),
        )
        assert ERROR_MESSAGE not in res.json()["detail"]

        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(
                package="fractal-tasks-core[standard]",
                package_version="99.99.99",
            ),
        )
        assert ERROR_MESSAGE in res.json()["detail"]
