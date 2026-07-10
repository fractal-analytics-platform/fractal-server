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


async def test_recent_activities(
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    job_factory,
    tmp_path,
    MockCurrentUser,
    db,
    capsys,
):
    MINUTES = 35
    now = get_timestamp()
    past = now - timedelta(minutes=MINUTES - 5)

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

        recent_activities(minutes=2)
        output = capsys.readouterr().out
        assert (
            "No fractal-server job or task-group activity during the last "
            "2 minutes."
        ) in output
        assert "## Recent Jobs" not in output
        assert "## Recent Task-Group activities" not in output

        recent_activities(minutes=MINUTES)
        output = capsys.readouterr().out
        assert (
            "There were fractal-server jobs and/or task-group activities "
            f"during the last {MINUTES} minutes."
        ) in output
        assert "## Recent jobs" in output
        assert "## Recent task-group activities" not in output
        output_splitted = output.split("\n")
        assert f"ID={job1.id}" in output_splitted[4]

        job2 = await job_factory(
            **job_args,
            status=JobStatusType.SUBMITTED,
            start_timestamp=now,
        )
        job3 = await job_factory(
            **job_args,
            status=JobStatusType.FAILED,
            start_timestamp=now,
            end_timestamp=now,
        )

        recent_activities(minutes=MINUTES)
        output = capsys.readouterr().out
        assert (
            "There are ongoing fractal-server jobs and/or task-group "
            "activities."
        ) in output
        assert "## Recent jobs" in output
        assert "## Recent task-group activities" not in output
        output_splitted = output.split("\n")
        assert f"ID={job2.id}" in output_splitted[4]
        assert f"ID={job3.id}" in output_splitted[5]
        assert f"ID={job1.id}" in output_splitted[6]

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
        act_5 = TaskGroupActivityV2(
            **activity_args,
            status=TaskGroupActivityStatus.OK,
            timestamp_started=now,
            timestamp_ended=now,
        )
        db.add_all([act_1, act_2, act_3, act_4, act_5])
        await db.commit()
        for act in [act_1, act_2, act_3, act_4, act_5]:
            await db.refresh(act)

    cmd = "fractalctl recent"
    res = subprocess.run(
        shlex.split(cmd),
        encoding="utf-8",
        capture_output=True,
    ).stdout.split("\n")

    assert len(res) == 13

    recent_activities(minutes=20)
    output = capsys.readouterr().out
    assert (
        "There are ongoing fractal-server jobs and/or task-group activities."
    ) in output
    assert "## Recent jobs" in output
    assert "## Recent task-group activities" in output
    output_splitted = output.split("\n")
    assert f"ID={job2.id}" in output_splitted[4]
    assert f"ID={job3.id}" in output_splitted[5]
    assert f"ID={act_1.id}" in output_splitted[8]
    assert f"ID={act_2.id}" in output_splitted[9]

    recent_activities(minutes=MINUTES)
    output = capsys.readouterr().out
    assert (
        "There are ongoing fractal-server jobs and/or task-group activities."
    ) in output
    assert "## Recent jobs" in output
    assert "## Recent task-group activities" in output
    output_splitted = output.split("\n")
    assert f"ID={job2.id}" in output_splitted[4]
    assert f"ID={job3.id}" in output_splitted[5]
    assert f"ID={job1.id}" in output_splitted[6]
    assert f"ID={act_1.id}" in output_splitted[9]
    assert f"ID={act_2.id}" in output_splitted[10]
    assert f"ID={act_5.id}" in output_splitted[11]
    assert f"ID={act_4.id}" in output_splitted[12]
