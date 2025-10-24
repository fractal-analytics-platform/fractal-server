import logging
from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.tasks.v2.ssh import collect_ssh
from fractal_server.tasks.v2.ssh import collect_ssh_pixi
from fractal_server.tasks.v2.ssh import deactivate_ssh
from fractal_server.tasks.v2.ssh import deactivate_ssh_pixi
from fractal_server.tasks.v2.ssh import reactivate_ssh
from fractal_server.tasks.v2.ssh import reactivate_ssh_pixi


async def test_unit_invalid_ssh(
    db,
    caplog,
    first_user,
    tmp777_path: Path,
    monkeypatch,
    slurm_ssh_resource_profile_fake_db,
):
    resource, profile = slurm_ssh_resource_profile_fake_db
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=(tmp777_path / "somewhere").as_posix(),
        user_id=first_user.id,
        resource_id=resource.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status="fake_status",
        action="fake_action",
        pkg_name="pkg",
        version="1.2.3",
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)

    def _patched_get_logger(logger_name: str) -> logging.Logger:
        logger = logging.getLogger(logger_name)
        logger.propagate = True
        return logger

    import fractal_server.tasks.v2.ssh._utils

    monkeypatch.setattr(
        fractal_server.tasks.v2.ssh._utils,
        "get_logger",
        _patched_get_logger,
    )

    for function, custom_args in [
        (collect_ssh, dict(wheel_file=None)),
        (collect_ssh_pixi, dict(tar_gz_file=None)),
        (deactivate_ssh, {}),
        (deactivate_ssh_pixi, {}),
        (reactivate_ssh, {}),
        (reactivate_ssh_pixi, {}),
    ]:
        caplog.clear()
        function(
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            resource=resource,
            profile=profile,
            **custom_args,
        )
        debug(caplog.text)
        assert "Cannot establish SSH connection" in caplog.text
