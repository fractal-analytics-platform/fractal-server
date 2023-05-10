import pytest
from fastapi import HTTPException

from fractal_server.app.api.v1._aux_functions import _check_workflow_exists
from fractal_server.app.api.v1._aux_functions import _get_dataset_check_owner
from fractal_server.app.api.v1._aux_functions import _get_job_check_owner
from fractal_server.app.api.v1._aux_functions import _get_project_check_owner
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
        task = await task_factory()
        wftask = await workflowtask_factory(
            workflow_id=workflow.id, task_id=task.id
        )
        other_workflow = await workflow_factory(project_id=project.id)
        other_task = await task_factory()
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


async def test_get_dataset_check_owner():
    pass


async def test_get_job_check_owner():
    pass
