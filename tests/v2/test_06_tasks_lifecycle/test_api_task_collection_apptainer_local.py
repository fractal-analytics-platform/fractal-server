from pathlib import Path

from fractal_server.tasks.config import TasksApptainerSettings


async def test_task_collection_apptainer_local(
    db,
    client,
    MockCurrentUser,
    pixi_pkg_targz: Path,
    local_resource_profile_db,
):
    """
    Apptainer task collection is only supported on SLURM-SSH resources, so it
    always fails on a local resource, with or without an apptainer config.
    """
    resource, profile = local_resource_profile_db

    with pixi_pkg_targz.open("rb") as f:
        files = {
            "file": (
                pixi_pkg_targz.name,
                f.read(),
                "application/gzip",
            )
        }

    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
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

        # Failure: apptainer is configured, but the resource is not SLURM-SSH
        res = await client.post(
            "api/v2/task/collect/apptainer/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "not available" in res.json()["detail"]
