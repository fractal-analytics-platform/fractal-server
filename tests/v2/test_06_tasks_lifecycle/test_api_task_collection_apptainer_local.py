import shutil
from pathlib import Path

from fractal_server.tasks.config import TasksApptainerSettings


async def test_task_collection_apptainer_local(
    db,
    client,
    MockCurrentUser,
    pixi_pkg_targz: Path,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    with pixi_pkg_targz.open("rb") as f:
        files = {
            "file": (
                pixi_pkg_targz.name,
                f.read(),
                "application/gzip",
            )
        }

    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        # Failure: apptainer is not configured for this resource
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "not available" in res.json()["detail"]

        resource.tasks_apptainer_config = TasksApptainerSettings(
            pixi_version="0.84.1",
            apptainer_bin="/usr/bin/apptainer",
        ).model_dump()
        db.add(resource)
        await db.commit()
        await db.refresh(resource)

        # Failure: local task-group folder already exists
        task_group_path = (
            Path(resource.tasks_local_dir)
            / str(user.id)
            / "mock-pixi-tasks"
            / "1.2.3"
        )
        task_group_path.mkdir(parents=True)
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "already exists" in res.json()["detail"]
        shutil.rmtree(task_group_path)

        # Success: endpoint accepts the request
        # FIXME the local background function is a no-op for now
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
