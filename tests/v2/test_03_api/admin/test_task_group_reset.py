import platform
from pathlib import Path

import pytest
from fastapi import HTTPException

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.admin.v2.task_group_reset import (
    _verify_reset_enabled_or_422,
)
from fractal_server.app.routes.admin.v2.task_group_reset import _verify_support
from fractal_server.app.routes.admin.v2.task_group_reset import (
    _verify_task_group_non_active_or_422,
)
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.v2.utils_pixi import SOURCE_DIR_NAME


async def test_unit_preliminary_checks(
    db,
    MockCurrentUser,
    override_settings_factory,
    slurm_ssh_resource_profile_fake_objects,
    task_factory,
):
    resource, profile = slurm_ssh_resource_profile_fake_objects
    override_settings_factory(FRACTAL_ENABLE_TASK_GROUP_RESET="false")
    with pytest.raises(HTTPException):
        _verify_reset_enabled_or_422()

    async with MockCurrentUser() as user:
        task_pixi = await task_factory(
            user_id=user.id,
            name="pixi",
            task_group_kwargs=dict(origin="pixi"),
        )
        task_group_pixi = await db.get(TaskGroupV2, task_pixi.taskgroupv2_id)
        task_wheel = await task_factory(
            user_id=user.id,
            name="wheel-file",
            task_group_kwargs=dict(origin="wheel-file"),
        )
        task_group_wheel = await db.get(TaskGroupV2, task_wheel.taskgroupv2_id)
    with pytest.raises(HTTPException, match="slurm_ssh not supported"):
        _verify_support(
            task_group=task_group_wheel,
            pip_or_pixi="pip",
            resource=resource,
        )

    with pytest.raises(HTTPException, match="Invalid task_group.origin"):
        _verify_support(
            task_group=task_group_pixi,
            pip_or_pixi="pip",
            resource=resource,
        )

    with pytest.raises(HTTPException, match="Please deactivate"):
        _verify_task_group_non_active_or_422(task_group=task_group_pixi)


@pytest.mark.parametrize("package_origin", ("pypi", "wheel", "pixi"))
@pytest.mark.skipif(
    platform.machine() == "arm64",
    reason="Pixi panics on ARM (osx-arm64 not in supported platforms)",
)
async def test_task_group_reset(
    package_origin,
    db,
    client,
    MockCurrentUser,
    current_py_version,
    override_settings_factory,
    local_resource_profile_db,
    pixi: TasksPixiSettings,
    pixi_pkg_targz: Path,
    testdata_path,
):
    resource, profile = local_resource_profile_db
    override_settings_factory(FRACTAL_ENABLE_TASK_GROUP_RESET="true")

    if package_origin == "pypi":
        pip_or_pixi = "pip"
        collect_request = dict(
            data=dict(
                package="testing-tasks-mock",
                python_version=current_py_version,
            )
        )
        reset_request = dict(
            python_version=current_py_version,
            pip_extras="",
        )
    elif package_origin == "wheel":
        pip_or_pixi = "pip"
        archive_path = (
            testdata_path
            / "testing-tasks-mock/dist"
            / "testing_tasks_mock-0.1.4-py3-none-any.whl"
        )
        with archive_path.open("rb") as f:
            collect_request = dict(
                files={
                    "file": (
                        archive_path.name,
                        f.read(),
                        "application/zip",
                    )
                }
            )
        reset_request = dict(
            python_version=current_py_version,
            pip_extras="",
        )
    else:
        pip_or_pixi = "pixi"
        with pixi_pkg_targz.open("rb") as f:
            collect_request = dict(
                files={
                    "file": (
                        pixi_pkg_targz.name,
                        f.read(),
                        "application/gzip",
                    )
                }
            )

        resource.tasks_pixi_config = pixi.model_dump()
        db.add(resource)
        await db.commit()
        reset_request = {}

    async with MockCurrentUser(profile_id=profile.id):
        res = await client.post(
            f"api/v2/task/collect/{pip_or_pixi}/", **collect_request
        )
        assert res.status_code == 202
        taskgroupv2_id = res.json()["taskgroupv2_id"]

        task_group = await db.get(TaskGroupV2, taskgroupv2_id)
        if package_origin == "pixi":
            internal_path = Path(task_group.path, SOURCE_DIR_NAME).as_posix()
        else:
            internal_path = Path(task_group.venv_path).as_posix()

        assert Path(internal_path).is_dir()

        # Simulate deactivation
        Path(internal_path).rename(f"{internal_path}-old")
        task_group.active = False
        db.add(task_group)
        await db.commit()

    async with MockCurrentUser(is_superuser=True):
        if package_origin == "pypi":
            original_version = task_group.version
            task_group.version = "999.999.999"
        elif package_origin in ("wheel", "pixi"):
            original_archive_path = task_group.archive_path
            task_group.archive_path = "/missing/file"
        db.add(task_group)
        await db.commit()

        res = await client.post(
            f"admin/v2/task-group/{taskgroupv2_id}/reset/{pip_or_pixi}/",
            json=reset_request,
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        activity = await db.get(TaskGroupActivityV2, activity_id)
        assert activity.status == "failed"
        assert not Path(internal_path).exists()

        if package_origin == "pypi":
            task_group.version = original_version
        elif package_origin in ("wheel", "pixi"):
            task_group.archive_path = original_archive_path
        db.add(task_group)
        await db.commit()

    async with MockCurrentUser(is_superuser=True):
        res = await client.post(
            f"admin/v2/task-group/{taskgroupv2_id}/reset/{pip_or_pixi}/",
            json=reset_request,
        )
        assert res.status_code == 202
        assert Path(internal_path).is_dir()
