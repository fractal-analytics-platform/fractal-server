import logging
import os

from fastapi import FastAPI
from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v1.job import ApplyWorkflow
from fractal_server.app.models.v2.job import JobV2
from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task as _workflow_insert_task_v1,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task as _workflow_insert_task_v2,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.main import lifespan
from fractal_server.ssh._fabric import FractalSSH
from tests.fixtures_slurm import SLURM_USER


async def test_app_with_lifespan(
    db,
    monkeypatch,
    override_settings_factory,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    project_factory,
    workflow_factory,
    dataset_factory,
    job_factory,
    task_factory,
    tmp_path,
):
    monkeypatch.setattr(
        "fractal_server.config.Settings.check_runner", lambda x: x
    )
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")
    app = FastAPI()
    res = await db.execute(select(UserOAuth))
    assert res.unique().all() == []

    async with lifespan(app):
        # verify first user creation
        res = await db.execute(select(UserOAuth))
        user = res.scalars().unique().all()
        assert len(user) == 1
        # verify shutdown
        assert len(app.state.jobsV1) == 0
        assert len(app.state.jobsV2) == 0

        task = await task_factory_v2(name="task", command="echo")
        project = await project_factory_v2(user[0])
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

        # create jobv1
        projectv1 = await project_factory(user[0])
        workflowv1 = await workflow_factory(project_id=projectv1.id)
        taskv1 = await task_factory(name="task", source="task_source")
        await _workflow_insert_task_v1(
            workflow_id=workflowv1.id, task_id=taskv1.id, db=db
        )
        datasetv1 = await dataset_factory(project_id=projectv1.id)
        jobv1 = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=datasetv1.id,
            output_dataset_id=datasetv1.id,
            workflow_id=workflow.id,
        )
        # append submitted job to jobsV2 status
        app.state.jobsV1.append(jobv1.id)
        # we need to close the db session to get
        # updated data from db
        await db.close()

    # verify that the shutdown file was created during the lifespan cleanup
    assert os.path.exists(f"{jobv2.working_dir}/{SHUTDOWN_FILENAME}")
    jobv2_after = (
        await db.execute(select(JobV2).where(JobV2.id == jobv2.id))
    ).scalar_one_or_none()
    jobv1_after = (
        await db.execute(
            select(ApplyWorkflow).where(ApplyWorkflow.id == jobv1.id)
        )
    ).scalar_one_or_none()
    assert jobv2_after.status == "failed"
    assert jobv1_after.status == "failed"
    assert jobv2_after.log == "\nJob stopped due to app shutdown\n"
    assert jobv1_after.log == "\nJob stopped due to app shutdown\n"


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

    async def raise_error(
        *, jobsV1: list[int], jobsV2: list[int], logger_name: str
    ):
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
        FRACTAL_SLURM_WORKER_PYTHON="/usr/bin/python3.9",
        FRACTAL_SLURM_SSH_HOST=slurmlogin_ip,
        FRACTAL_SLURM_SSH_USER=SLURM_USER,
        FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH=ssh_keys["private"],
        FRACTAL_SLURM_SSH_WORKING_BASE_DIR=(
            tmp777_path / "artifacts"
        ).as_posix(),
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )
    app = FastAPI()
    res = await db.execute(select(UserOAuth))
    assert res.unique().all() == []

    async with lifespan(app):
        # verify first user creation
        res = await db.execute(select(UserOAuth))
        user = res.scalars().unique().all()
        assert len(user) == 1
        # verify shutdown
        assert len(app.state.jobsV1) == 0
        assert len(app.state.jobsV2) == 0
        assert isinstance(app.state.fractal_ssh, FractalSSH)
        app.state.fractal_ssh.check_connection()
