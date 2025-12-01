from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityAction
from fractal_server.tasks.v2.local import collect_local_pixi


async def test_collect_local_pixi_path_exists(
    override_settings_factory,
    tmp_path: Path,
    db,
    first_user,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    resource.tasks_pixi_config = dict(
        default_version="x",
        versions={"x": "/fake/x"},
    )
    db.add(resource)
    await db.commit()

    # Prepare db objects
    path = tmp_path / "something"
    path.mkdir()
    task_group = TaskGroupV2(
        pkg_name="mock-pixi-tasks",
        version="0.2.1",
        origin="pixi",
        path=path.as_posix(),
        user_id=first_user.id,
        pixi_version="x",
        resource_id=resource.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.COLLECT,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)

    # Run background task
    collect_local_pixi(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        tar_gz_file=FractalUploadedFile(
            contents=b"fake",
            filename="fake",
        ),
        resource=resource,
        profile=profile,
    )
    # Verify that collection failed
    task_group_activity = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity)
    assert task_group_activity.status == "failed"
    assert task_group_activity.taskgroupv2_id is None
