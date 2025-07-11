import shutil
from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.config import get_settings
from fractal_server.config import PixiSettings
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils_pixi import SOURCE_DIR_NAME


async def test_pixi_not_available(client, MockCurrentUser):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "9.9.9"},
            files={"file": ("name", b"", "application/gzip")},
        )
        assert res.status_code == 422
        assert res.json()["detail"] == "Pixi task collection is not available."


async def test_api_failures(
    override_settings_factory,
    client,
    MockCurrentUser,
    tmp_path: Path,
):
    override_settings_factory(
        FRACTAL_PIXI_CONFIG_FILE="/fake/pixi/pixi.json",
        pixi=PixiSettings(
            default_version="1.0.0",
            versions={
                "1.0.0": "/fake/pixi/1.0.0",
                "1.0.1": "/fake/pixi/1.0.1",
            },
        ),
    )

    def empty_tar_gz(filename) -> dict:
        valid_tar_gz = tmp_path / f"{filename}.tar.gz"
        valid_tar_gz.touch()
        with open(valid_tar_gz, "rb") as f:
            tar_gz_content = f.read()
        return {
            "file": (
                valid_tar_gz.name,
                tar_gz_content,
                "application/gzip",
            )
        }

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # no data nor files
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files={},
        )
        assert res.status_code == 422

        # too many hyphens
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage-0.1.2-a345"),
        )
        assert res.status_code == 422

        # no hyphen
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage0.1.2a345"),
        )
        assert res.status_code == 422

        # pixi version not available
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage-0.1.2a345"),
        )
        assert res.status_code == 422


async def test_pixi_collection_path_already_exists(
    override_settings_factory,
    client,
    MockCurrentUser,
    pixi: PixiSettings,
    pixi_pkg_targz: Path,
    tmp_path: Path,
):
    override_settings_factory(
        FRACTAL_TASKS_DIR=tmp_path,
        FRACTAL_PIXI_CONFIG_FILE="/fake/file",
        pixi=pixi,
    )

    with pixi_pkg_targz.open("rb") as f:
        files = {
            "file": (
                pixi_pkg_targz.name,
                f.read(),
                "application/gzip",
            )
        }

    settings = Inject(get_settings)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        task_group_path = (
            Path(settings.FRACTAL_TASKS_DIR.as_posix())
            / str(user.id)
            / "mock-pixi-tasks"
            / "0.2.1"
        )
        task_group_path.mkdir(parents=True)
        debug(task_group_path)

        # Trigger task collection
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "already exists" in res.json()["detail"]


async def test_task_group_lifecycle_pixi_local(
    override_settings_factory,
    client,
    MockCurrentUser,
    pixi: PixiSettings,
    pixi_pkg_targz: Path,
    tmp_path: Path,
    db,
):
    override_settings_factory(
        FRACTAL_PIXI_CONFIG_FILE="/fake/file",
        FRACTAL_TASKS_DIR=tmp_path,
        pixi=pixi,
    )

    with pixi_pkg_targz.open("rb") as f:
        files = {
            "file": (
                pixi_pkg_targz.name,
                f.read(),
                "application/gzip",
            )
        }

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Successful collection
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files=files,
        )
        assert res.status_code == 202
        assert res.json()["status"] == "pending"
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["timestamp_ended"] is not None
        assert activity["log"] is not None
        assert activity["status"] == "OK"
        task_group_id = activity["taskgroupv2_id"]
        # Check `TaskGroupV2.env_info` (only available through database)
        db.expunge_all()
        task_group = await db.get(TaskGroupV2, task_group_id)
        assert len(task_group.task_list) == 1
        assert task_group.venv_size_in_kB is not None
        assert task_group.venv_file_number is not None
        assert task_group.env_info is not None
        # Check `TaskGroupReadV2.task_list` (only available through API)
        res = await client.get(f"/api/v2/task-group/{task_group_id}/")
        assert res.status_code == 200
        task = res.json()["task_list"][0]
        module_path = task["command_non_parallel"].split()[-1]
        debug(task["command_non_parallel"])
        debug(module_path)
        assert Path(module_path).is_file()

        # Failed collection - due to non-duplication constraint
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "already owns a task group" in str(res.json()["detail"])

        # Successful deactivation
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/deactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["status"] == "OK"
        assert Path(task_group.archive_path).exists()
        assert not Path(task_group.path, SOURCE_DIR_NAME).exists()

        # Failed deactivation (folder does not exist)
        db.expunge_all()
        task_group.active = True  # mock an active task group
        await db.merge(task_group)
        await db.commit()
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/deactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        assert activity["status"] == "failed"
        db.expunge_all()
        task_group.active = False  # restore `active=False`
        await db.merge(task_group)
        await db.commit()

        # Failed reactivation - (fake) folder already exists
        fake_remote_dir = Path(task_group.path, SOURCE_DIR_NAME)
        fake_remote_dir.mkdir()  # Create fake folder
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/reactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        debug(activity)
        assert activity["status"] == "failed"
        shutil.rmtree(fake_remote_dir.as_posix())  # Remove fake folder

        # Successful reactivation
        res = await client.post(
            f"/api/v2/task-group/{task_group_id}/reactivate/",
            data={},
        )
        assert res.status_code == 202
        activity_id = res.json()["id"]
        res = await client.get(f"/api/v2/task-group/activity/{activity_id}/")
        assert res.status_code == 200
        activity = res.json()
        debug(activity)
        assert activity["status"] == "OK"
