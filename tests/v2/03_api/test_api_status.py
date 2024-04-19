import json

from devtools import debug

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.filenames import HISTORY_FILENAME


async def test_workflowtask_status_no_history_no_job(
    db,
    MockCurrentUser,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    client,
):
    history = []
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(name="task1", source="task1")
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {}}


async def test_workflowtask_status_history_no_job(
    db,
    MockCurrentUser,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    client,
):
    history = []
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(name="task1", source="task1")
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")

        # (1)
        for dummy_status in ["done", "failed", "done"]:
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )
            history.append(
                dict(
                    workflowtask=dict(id=workflow.task_list[-1].id),
                    status=dummy_status,
                )
            )

        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {"1": "done", "2": "failed"}}

        # (2)
        wf_task_id = history[1]["workflowtask"]["id"]

        res = await client.delete(
            f"api/v2/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"{wf_task_id}/"
        )
        assert res.status_code == 204

        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.json() == {"status": {"1": "done", "3": "done"}}
        # (3)
        history.pop(1)
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {"1": "done", "3": "done"}}

        # (4)
        history.append(
            dict(workflowtask=dict(id=workflow.task_list[0].id), status="done")
        )
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {"1": "done"}}


async def test_workflowtask_status_history_job(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    client,
):
    working_dir = tmp_path / "working_dir"
    history = []
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(name="task1", source="task1")
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")

        for _ in range(3):
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )

        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )

        await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=str(working_dir),
            first_task_index=0,
            last_task_index=1,
        )

    res = await client.get(
        (
            f"api/v2/project/{project.id}/status/?"
            f"dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
    )
    assert res.status_code == 200
    assert res.json() == {"status": {"1": "submitted", "2": "submitted"}}

    history = [dict(workflowtask=dict(id=1), status="done")]
    working_dir.mkdir()
    with (working_dir / HISTORY_FILENAME).open("w") as f:
        json.dump(history, f)
    debug(working_dir / HISTORY_FILENAME)
    res = await client.get(
        (
            f"api/v2/project/{project.id}/status/?"
            f"dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
    )
    assert res.status_code == 200
    assert res.json() == {"status": {"1": "done", "2": "submitted"}}


async def test_workflowtask_status_two_jobs(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    client,
):
    working_dir = tmp_path / "working_dir"
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, history=[])
        task = await task_factory_v2(name="task1", source="task1")
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        for _ in range(2):
            await job_factory_v2(
                project_id=project.id,
                workflow_id=workflow.id,
                dataset_id=dataset.id,
                working_dir=str(working_dir),
            )

    res = await client.get(
        (
            f"api/v2/project/{project.id}/status/?"
            f"dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
    )
    debug(res.json())
    assert res.status_code == 422
