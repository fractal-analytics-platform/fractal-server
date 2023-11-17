import pytest
from fastapi import HTTPException

from fractal_server.app.api.v1._aux_functions import _check_workflow_exists
from fractal_server.app.api.v1._aux_functions import _get_active_jobs_statement
from fractal_server.app.api.v1._aux_functions import _get_dataset_check_owner
from fractal_server.app.api.v1._aux_functions import _get_job_check_owner
from fractal_server.app.api.v1._aux_functions import _get_project_check_owner
from fractal_server.app.api.v1._aux_functions import _get_task_check_owner
from fractal_server.app.api.v1._aux_functions import _get_workflow_check_owner
from fractal_server.app.api.v1._aux_functions import (
    _get_workflow_task_check_owner,
)


async def test_get_project_check_owner(
    MockCurrentUser,
    project_factory,
    db,
):
    async with MockCurrentUser(persist=True) as other_user:
        other_project = await project_factory(other_user)

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)

        # Test success
        await _get_project_check_owner(
            project_id=project.id, user_id=user.id, db=db
        )

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_project_check_owner(
                project_id=project.id + 1, user_id=user.id, db=db
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Project not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_project_check_owner(
                project_id=other_project.id, user_id=user.id, db=db
            )
        assert err.value.status_code == 403
        assert err.value.detail == f"Not allowed on project {other_project.id}"


async def test_get_workflow_check_owner(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    db,
):
    async with MockCurrentUser(persist=True) as other_user:
        other_project = await project_factory(other_user)
        other_workflow = await workflow_factory(project_id=other_project.id)

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)

        # Test success
        await _get_workflow_check_owner(
            project_id=project.id,
            workflow_id=workflow.id,
            user_id=user.id,
            db=db,
        )

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_workflow_check_owner(
                project_id=project.id,
                workflow_id=workflow.id + 1,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Workflow not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_workflow_check_owner(
                project_id=project.id,
                workflow_id=other_workflow.id,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 422
        assert err.value.detail == (
            f"Invalid project_id={project.id} "
            f"for workflow_id={other_workflow.id}."
        )


async def test_get_workflow_task_check_owner(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    task_factory,
    workflowtask_factory,
    db,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(source="A")
        wftask = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        other_workflow = await workflow_factory(project_id=project.id)
        other_task = await task_factory(source="B")
        other_wftask = await workflowtask_factory(
            workflow_id=other_workflow.id, task_id=other_task.id
        )

        # Test success
        await _get_workflow_task_check_owner(
            project_id=project.id,
            workflow_id=workflow.id,
            workflow_task_id=wftask.id,
            user_id=user.id,
            db=db,
        )
        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_workflow_task_check_owner(
                project_id=project.id,
                workflow_id=workflow.id,
                workflow_task_id=wftask.id + other_wftask.id,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "WorkflowTask not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_workflow_task_check_owner(
                project_id=project.id,
                workflow_id=workflow.id,
                workflow_task_id=other_wftask.id,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 422
        assert err.value.detail == (
            f"Invalid workflow_id={workflow.id} "
            f"for workflow_task_id={other_wftask.id}"
        )


async def test_check_workflow_exists(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    db,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)

    # Test success
    await _check_workflow_exists(
        name=workflow.name + "abc",
        project_id=project.id,
        db=db,
    )
    await _check_workflow_exists(
        name=workflow.name,
        project_id=project.id + 1,
        db=db,
    )

    # Test fail
    with pytest.raises(HTTPException) as err:
        await _check_workflow_exists(
            name=workflow.name,
            project_id=project.id,
            db=db,
        )
        assert err.value.status_code == 404
        assert err.value.detail == (
            f"Workflow with name={workflow.name} and project_id={project.id} "
            "already in use"
        )


async def test_get_dataset_check_owner(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    db,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        other_project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)

        # Test success
        await _get_dataset_check_owner(
            project_id=project.id,
            dataset_id=dataset.id,
            user_id=user.id,
            db=db,
        )

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_dataset_check_owner(
                project_id=project.id,
                dataset_id=dataset.id + 1,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Dataset not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_dataset_check_owner(
                project_id=other_project.id,
                dataset_id=dataset.id,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 422
        assert err.value.detail == (
            f"Invalid project_id={other_project.id} "
            f"for dataset_id={dataset.id}"
        )


async def test_get_job_check_owner(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    dataset_factory,
    job_factory,
    task_factory,
    db,
    tmp_path,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user, id=1)
        other_project = await project_factory(user, id=2)

        workflow = await workflow_factory(project_id=project.id)
        t = await task_factory()
        await workflow.insert_task(task_id=t.id, db=db)

        dataset = await dataset_factory(project_id=project.id)

        job = await job_factory(
            project_id=project.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path,
        )

        # Test success
        await _get_job_check_owner(
            project_id=project.id, job_id=job.id, user_id=user.id, db=db
        )

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_job_check_owner(
                project_id=project.id,
                job_id=job.id + 1,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Job not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_job_check_owner(
                project_id=other_project.id,
                job_id=job.id,
                user_id=user.id,
                db=db,
            )
        assert err.value.status_code == 422
        assert err.value.detail == (
            f"Invalid project_id={other_project.id} for job_id={job.id}"
        )


async def test_get_task_check_owner(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    task_factory,
    workflowtask_factory,
    db,
):
    async with MockCurrentUser(user_kwargs={"username": "alice"}) as user:
        taskA = await task_factory(source="A", owner=user.username)
        taskB = await task_factory(source="B")

        # Test fail 1: 404 NOT FOUND
        with pytest.raises(HTTPException) as err:
            await _get_task_check_owner(
                task_id=taskA.id + 999, user=user, db=db
            )
        assert err.value.status_code == 404
        assert err.value.detail == f"Task {taskA.id + 999} not found."

        # Test success
        _task = await _get_task_check_owner(task_id=taskA.id, user=user, db=db)
        assert _task.id == taskA.id

        # Test fail 2: 403 FORBIDDEN
        with pytest.raises(HTTPException) as err:
            await _get_task_check_owner(task_id=taskB.id, user=user, db=db)
        assert err.value.status_code == 403
        assert err.value.detail == (
            "Only a superuser can modify a Task with `owner=None`."
        )

    async with MockCurrentUser(user_kwargs={"username": "bob"}) as user:
        # Test fail 3: 403 FORBIDDEN
        with pytest.raises(HTTPException) as err:
            await _get_task_check_owner(task_id=taskA.id, user=user, db=db)
        assert err.value.status_code == 403
        assert err.value.detail == (
            f"Current user ({user.username}) cannot modify Task {taskA.id} "
            f"with different owner ({taskA.owner})."
        )

    async with MockCurrentUser(
        user_kwargs={"username": "boss", "is_superuser": True}
    ) as superuser:
        # Test success
        _task = await _get_task_check_owner(
            task_id=taskA.id, user=superuser, db=db
        )
        assert _task.id == taskA.id
        _task = await _get_task_check_owner(
            task_id=taskB.id, user=superuser, db=db
        )
        assert _task.id == taskB.id


async def test_get_active_jobs_statement():

    stm = _get_active_jobs_statement()
    from sqlmodel.sql.expression import SelectOfScalar

    assert type(stm) is SelectOfScalar
