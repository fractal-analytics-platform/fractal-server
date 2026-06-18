import os
import shlex
import signal
import subprocess
import time
from datetime import timedelta

import pytest
from devtools import debug

from fractal_server.__main__ import recent_activities
from fractal_server.app.models.v2.task_group import TaskGroupActivityV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2.job import JobStatusType
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatus
from fractal_server.utils import get_timestamp

commands = [
    "fractalctl start --reload --host 0.0.0.0 --port 8000",
    (
        "gunicorn fractal_server.main:app "
        "--workers 1 "
        "--bind 0.0.0.0:8000 "
        "--worker-class fractal_server.gunicorn_fractal.FractalWorker "
        "--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger "
    ),
]


@pytest.mark.parametrize("cmd", commands)
def test_startup_commands(cmd, db_create_tables):
    debug(cmd)
    p = subprocess.Popen(
        shlex.split(f"uv run --frozen {cmd}"),
        start_new_session=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )
    debug(p)
    with pytest.raises(subprocess.TimeoutExpired) as e:
        p.wait(timeout=1.5)
        out, err = p.communicate()
        debug(p.returncode)
        debug(out)
        debug(err)
        assert p.returncode == 0

    debug("Now call killpg")
    debug(p)
    os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    # Wait a bit, so that the killpg ends before pytest ends
    time.sleep(0.3)
    debug(e.value)


def test_email_settings():
    cmd = "uv run --frozen fractalctl email-settings"
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
    )
    assert not res.stdout
    assert "usage" in res.stderr


async def test_recent_activities(
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    job_factory,
    tmp_path,
    MockCurrentUser,
    db,
):
    recent_activities(minutes=35)  # for coverage

    now = get_timestamp()
    past = now - timedelta(minutes=30)

    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id, name="task")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db, order=0
        )
        job_args = dict(
            working_dir=f"{tmp_path.as_posix()}/x",
            working_dir_user=f"{tmp_path.as_posix()}/y",
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            user_email=user.email,
        )
        job1 = await job_factory(
            **job_args,
            status=JobStatusType.FAILED,
            start_timestamp=past,
            end_timestamp=past,
        )
        recent_activities(minutes=35)  # for coverage
        job2 = await job_factory(
            **job_args,
            status=JobStatusType.SUBMITTED,
            start_timestamp=now,
        )
        recent_activities(minutes=35)  # for coverage
        activity_args = dict(
            user_id=user.id,
            taskgroupv2_id=task.taskgroupv2_id,
            pkg_name="a",
            version="b",
            action="c",
        )
        act_1 = TaskGroupActivityV2(
            **activity_args,
            status=TaskGroupActivityStatus.ONGOING,
        )
        act_2 = TaskGroupActivityV2(
            **activity_args,
            status=TaskGroupActivityStatus.PENDING,
        )
        act_3 = TaskGroupActivityV2(
            **activity_args,
            status=TaskGroupActivityStatus.OK,
        )
        act_4 = TaskGroupActivityV2(
            **activity_args,
            status=TaskGroupActivityStatus.OK,
            timestamp_started=past,
            timestamp_ended=past,
        )
        db.add_all([act_1, act_2, act_3, act_4])
        await db.commit()
        for act in [act_1, act_2, act_3, act_4]:
            await db.refresh(act)

    cmd = "fractalctl recent"
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
    ).stdout.split("\n")
    assert len(res) == 9
    assert f"ID={job2.id}" in res[4]
    assert f"ID={act_1.id}" in res[6]
    assert f"ID={act_2.id}" in res[7]

    cmd = "fractalctl recent --minutes 35"
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
    ).stdout.split("\n")
    assert len(res) == 11
    assert f"ID={job2.id}" in res[4]
    assert f"ID={job1.id}" in res[5]
    assert f"ID={act_1.id}" in res[7]
    assert f"ID={act_2.id}" in res[8]
    assert f"ID={act_4.id}" in res[9]
