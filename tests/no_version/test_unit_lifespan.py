import os

from fastapi import FastAPI
from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.main import lifespan


async def test_app_with_lifespan(
    db,
    override_settings_factory,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
):
    override_settings_factory(FRACTAL_GRACEFUL_SHUTDOWN_TIME=0)

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
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # Create job with submitted status
        job = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset1.id,
            status="submitted",
            working_dir="/tmp",
            last_task_index=0,
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)
        # append submitted job to jobsV2 status
        app.state.jobsV2.append(job.id)
    # verify that the shutdown file was created during the lifespan cleanup
    assert os.path.exists(f"{job.working_dir}/{SHUTDOWN_FILENAME}")
