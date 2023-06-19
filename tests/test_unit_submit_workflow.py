import datetime
import time
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import ApplyWorkflow
from fractal_server.app.runner import submit_workflow


async def test_success_submit_workflows(
    MockCurrentUser,
    db,
    project_factory,
    workflow_factory,
    dataset_factory,
    job_factory,
    resource_factory,
    tmp_path,
):
    """
    WHEN `submit_worflow` is called twice at different times
    THEN two different folders are created
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        dataset = await dataset_factory(project)
        job0 = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            working_id=workflow.id,
        )
        await resource_factory(dataset)

        await submit_workflow(
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            job_id=job0.id,
        )
        with next(get_sync_db()) as _db:
            job1 = _db.get(ApplyWorkflow, job0.id)
        debug(job1.working_dir)
        folder1 = Path(job1.working_dir).name

        time.sleep(1.01)

        await submit_workflow(
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            job_id=job0.id,
        )
        await db.close()
        with next(get_sync_db()) as _db:
            job2 = _db.get(ApplyWorkflow, job0.id)
        debug(job2.working_dir)
        folder2 = Path(job2.working_dir).name

        debug(folder1, folder2)
        assert folder1 != folder2


async def test_fail_submit_workflows_at_same_time(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    dataset_factory,
    job_factory,
    resource_factory,
    tmp_path,
    monkeypatch,
):
    """
    WHEN `submit_worflow` is called twice at the "same time" (monkeypatched)
    THEN a RuntimeError is raised
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        dataset = await dataset_factory(project)
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            working_id=workflow.id,
        )
        await resource_factory(dataset)

        def patched_get_timestamp():
            return datetime.datetime(
                3000, 1, 1, 0, 0, tzinfo=datetime.timezone.utc
            )

        monkeypatch.setattr(
            "fractal_server.app.runner.get_timestamp", patched_get_timestamp
        )

        await submit_workflow(
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            job_id=job.id,
        )
        with pytest.raises(RuntimeError):
            await submit_workflow(
                workflow_id=workflow.id,
                input_dataset_id=dataset.id,
                output_dataset_id=dataset.id,
                job_id=job.id,
            )
