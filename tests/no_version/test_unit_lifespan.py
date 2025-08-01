import logging
import os

import pytest
from fastapi import FastAPI
from sqlmodel import select

from fractal_server.app.models import UserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2.job import JobV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task as _workflow_insert_task_v2,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.security import _create_first_group
from fractal_server.app.security import _create_first_user
from fractal_server.config import get_settings
from fractal_server.main import lifespan
from fractal_server.ssh._fabric import FractalSSHList
from fractal_server.syringe import Inject


async def test_app_with_lifespan(
    db,
    monkeypatch,
    override_settings_factory,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    tmp_path,
):
    monkeypatch.setattr(
        "fractal_server.config.Settings.check_runner", lambda x: x
    )
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")
    app = FastAPI()
    res = await db.execute(select(UserOAuth))
    assert res.unique().all() == []

    # create first user
    settings = Inject(get_settings)

    _create_first_group()
    await _create_first_user(
        email=settings.FRACTAL_DEFAULT_ADMIN_EMAIL,
        password=settings.FRACTAL_DEFAULT_ADMIN_PASSWORD.get_secret_value(),
        username=settings.FRACTAL_DEFAULT_ADMIN_USERNAME,
        is_superuser=True,
        is_verified=True,
    )
    res = await db.execute(select(UserOAuth))
    user = res.scalars().unique().one()  # assert only one user
    res = await db.execute(select(UserGroup))
    res.scalars().unique().one()  # assert only one group

    # db.add(LinkUserGroup(user_id=user.id, group_id=group.id))
    # await db.commit()

    async with lifespan(app):
        # verify shutdown
        assert len(app.state.jobsV2) == 0

        task = await task_factory_v2(
            user_id=user.id, name="task", command="echo"
        )
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        dataset1 = await dataset_factory_v2(project_id=project.id, name="ds-1")
        await _workflow_insert_task_v2(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        # Create jobv2 with submitted status
        jobv2 = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset1.id,
            status="submitted",
            working_dir=tmp_path.as_posix(),
            last_task_index=0,
        )

        # append submitted job to jobsV2 status
        app.state.jobsV2.append(jobv2.id)

        # we need to close the db session to get
        # updated data from db
        await db.close()

    # verify that the shutdown file was created during the lifespan cleanup
    assert os.path.exists(f"{jobv2.working_dir}/{SHUTDOWN_FILENAME}")
    jobv2_after = (
        await db.execute(select(JobV2).where(JobV2.id == jobv2.id))
    ).scalar_one_or_none()

    assert jobv2_after.status == "failed"
    assert jobv2_after.log == "\nJob stopped due to app shutdown\n"


async def test_lifespan_shutdown_empty_jobs_list(
    override_settings_factory,
    monkeypatch,
    caplog,
    db,
):
    monkeypatch.setattr(
        "fractal_server.config.Settings.check_runner", lambda x: x
    )
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")
    caplog.set_level(logging.INFO)
    app = FastAPI()
    async with lifespan(app):
        logger = logging.getLogger("fractal_server.lifespan")
        logger.propagate = True

    log_text = (
        "All jobs associated to this app are either done or failed. Exit."
    )
    assert any(record.message == log_text for record in caplog.records)


async def test_lifespan_shutdown_raise_error(
    override_settings_factory,
    monkeypatch,
    caplog,
    db,
):
    monkeypatch.setattr(
        "fractal_server.config.Settings.check_runner", lambda x: x
    )

    # mock function to trigger except

    async def raise_error(*, jobsV2: list[int], logger_name: str):
        raise ValueError("ERROR")

    monkeypatch.setattr(
        "fractal_server.main.cleanup_after_shutdown", raise_error
    )

    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")
    caplog.set_level(logging.INFO)
    app = FastAPI()
    async with lifespan(app):
        logger = logging.getLogger("fractal_server.lifespan")
        logger.propagate = True

    log_text = (
        "Something went wrong during shutdown phase, "
        "some of running jobs are not shutdown properly. "
        "Original error: ERROR"
    )
    from devtools import debug

    debug(caplog.records)
    assert any(record.message == log_text for record in caplog.records)


@pytest.mark.container
@pytest.mark.ssh
async def test_lifespan_slurm_ssh(
    override_settings_factory,
    slurmlogin_ip,
    ssh_keys: dict[str, str],
    tmp777_path,
    testdata_path,
    db,
):
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm_ssh",
        FRACTAL_SLURM_WORKER_PYTHON="/not/relevant",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )
    app = FastAPI()
    async with lifespan(app):
        assert len(app.state.jobsV2) == 0
        assert isinstance(app.state.fractal_ssh_list, FractalSSHList)
        assert app.state.fractal_ssh_list.size == 0
