from devtools import debug
from sqlalchemy.orm.attributes import flag_modified

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2.dataset import WorkflowTaskStatusTypeV2


async def test_workflowtask_status_no_history_no_job(
    db,
    MockCurrentUser,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    client,
):
    """
    Test the status endpoint when there is information in the DB and no running
    job associated to a given dataset/workflow pair.
    """
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(
            user_id=user.id, name="task", source="task1"
        )
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id, history=[])
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
    """
    Test the status endpoint when there is a non-empty history in the DB but
    no running job associated to a given dataset/workflow pair.
    """
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(
            user_id=user.id, name="task1", source="task1"
        )
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")

        # CASE 1
        # Prepare history
        history = []
        for dummy_status in ["done", "failed", "done"]:
            wftask = await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )
            history.append(
                dict(
                    workflowtask=dict(id=wftask.id),
                    status=dummy_status,
                )
            )
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        # Test the endpoint
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {"1": "done", "2": "failed"}}

        # CASE 2
        # Delete an entry from the history
        history.pop(1)
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        # Test the endpoint
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {"1": "done", "3": "done"}}

        # CASE 3
        # Re-append the first item at the end of the history
        history.append(history[0])
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        # Test the endpoint
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.status_code == 200
        assert res.json() == {"status": {"1": "done"}}

        # CASE 4
        # Delete a wftask from the workflow
        wf_task_id = history[0]["workflowtask"]["id"]
        res = await client.delete(
            f"api/v2/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"{wf_task_id}/"
        )
        assert res.status_code == 204
        # Test the endpoint
        res = await client.get(
            (
                f"api/v2/project/{project.id}/status/?"
                f"dataset_id={dataset.id}&workflow_id={workflow.id}"
            )
        )
        assert res.json() == {"status": {"3": "done"}}


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
    """
    Test the status endpoint when there is a some history in the DB and
    there is a running job associated to a given dataset/workflow pair.
    """
    working_dir = tmp_path / "working_dir"
    history = [
        dict(
            workflowtask=dict(id=1),
            status=WorkflowTaskStatusTypeV2.SUBMITTED,
        )
    ]
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        task = await task_factory_v2(
            user_id=user.id, name="task1", source="task1"
        )

        workflow = await workflow_factory_v2(project_id=project.id, name="WF")
        for _ in range(3):
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )
        await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=str(working_dir),
            first_task_index=0,
            last_task_index=1,
        )

    # CASE 1: first submitted
    res = await client.get(
        (
            f"api/v2/project/{project.id}/status/?"
            f"dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
    )
    assert res.status_code == 200
    assert res.json()["status"] == {"1": "submitted", "2": "submitted"}

    # CASE 2: first done
    dataset.history = [
        dict(
            workflowtask=dict(id=workflow.task_list[0].id),
            status=WorkflowTaskStatusTypeV2.DONE,
        ),
        dict(
            workflowtask=dict(id=workflow.task_list[1].id),
            status=WorkflowTaskStatusTypeV2.SUBMITTED,
        ),
    ]
    flag_modified(dataset, "history")
    await db.merge(dataset)
    await db.commit()
    res = await client.get(
        (
            f"api/v2/project/{project.id}/status/?"
            f"dataset_id={dataset.id}&workflow_id={workflow.id}"
        )
    )
    assert res.status_code == 200
    assert res.json()["status"] == {"1": "done", "2": "submitted"}


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
    """
    If there are more than one jobs associated to a given dataset/workflow pair
    (which should never happen), the endpoint responds with 422.
    """
    working_dir = tmp_path / "working_dir"
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, history=[])
        task = await task_factory_v2(
            user_id=user.id, name="task1", source="task1"
        )
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


async def test_workflowtask_status_modified_workflow(
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
    """
    Test the status endpoint when there is a running job associated to a given
    dataset/workflow pair, but the workflow is modified after the job was
    created.
    """
    working_dir = tmp_path / "working_dir"
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, history=[])
        task = await task_factory_v2(
            user_id=user.id, name="task1", source="task1"
        )
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")
        for _ in range(3):
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )
        await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=str(working_dir),
            first_task_index=0,
            last_task_index=1,
        )

        # Delete second and third WorkflowTasks
        await client.get(
            f"api/v2/project/{project.id}/workflow/{workflow.id}/"
        )
