from pathlib import Path

from fractal_server.app.models.v2 import TaskGroupV2


async def test_reset(
    db,
    client,
    MockCurrentUser,
    current_py_version,
    override_settings_factory,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    async with MockCurrentUser(profile_id=profile.id):
        res = await client.post(
            "api/v2/task/collect/pip/",
            data=dict(
                package="testing-tasks-mock",
                python_version=current_py_version,
            ),
        )
        assert res.status_code == 202
        taskgroupv2_id = res.json()["taskgroupv2_id"]

        task_group = await db.get(TaskGroupV2, taskgroupv2_id)
        assert Path(task_group.venv_path).is_dir()

        res = await client.post(
            f"api/v2/task-group/{taskgroupv2_id}/deactivate/"
        )
        assert res.status_code == 202
        assert not Path(task_group.venv_path).exists()

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
                pinned_package_versions_pre='{"devtools": "0.12.1"}',
            ),
        )
        assert res.status_code == 202
        assert Path(task_group.venv_path).is_dir()

        res = await client.post(
            f"admin/v2/task-group/{taskgroupv2_id}/reset/pip/", json={}
        )
        assert res.status_code == 422
        assert "Please deactivate" in res.json()["detail"]
