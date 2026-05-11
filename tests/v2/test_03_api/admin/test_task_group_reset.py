from pathlib import Path

import pytest

from fractal_server.app.models.v2 import TaskGroupV2


@pytest.mark.parametrize("package_origin", ("pypi", "wheel"))
async def test_reset_pip(
    package_origin,
    db,
    client,
    MockCurrentUser,
    current_py_version,
    override_settings_factory,
    local_resource_profile_db,
    testdata_path,
):
    resource, profile = local_resource_profile_db

    if package_origin == "pypi":
        request = dict(
            data=dict(
                package="testing-tasks-mock",
                python_version=current_py_version,
            )
        )
    else:
        archive_path = (
            testdata_path
            / "testing-tasks-mock/dist"
            / "testing_tasks_mock-0.1.4-py3-none-any.whl"
        )
        with archive_path.open("rb") as f:
            request = dict(
                files={
                    "file": (
                        archive_path.name,
                        f.read(),
                        "application/zip",
                    )
                }
            )

    async with MockCurrentUser(profile_id=profile.id):
        res = await client.post("api/v2/task/collect/pip/", **request)
        assert res.status_code == 202
        taskgroupv2_id = res.json()["taskgroupv2_id"]

        task_group = await db.get(TaskGroupV2, taskgroupv2_id)
        assert Path(task_group.venv_path).is_dir()

        # Simulate deactivation
        Path(task_group.venv_path).rename(f"{task_group.venv_path}-old")
        task_group.active = False
        db.add(task_group)
        await db.commit()

    async with MockCurrentUser(is_superuser=True):
        res = await client.post(
            f"admin/v2/task-group/{taskgroupv2_id}/reset/pip/", json={}
        )
        assert res.status_code == 422
        assert "FRACTAL_ENABLE_TASK_GROUP_RESET" in res.json()["detail"]

        override_settings_factory(FRACTAL_ENABLE_TASK_GROUP_RESET="true")

        res = await client.post(
            f"admin/v2/task-group/{taskgroupv2_id}/reset/pip/",
            json=dict(
                python_version=current_py_version,
                pip_extras="",
            ),
        )
        assert res.status_code == 202
        assert Path(task_group.venv_path).is_dir()

        res = await client.post(
            f"admin/v2/task-group/{taskgroupv2_id}/reset/pip/", json={}
        )
        assert res.status_code == 422
        assert "Please deactivate" in res.json()["detail"]
