import pytest
from fastapi import HTTPException

from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import ProjectV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions import (
    _check_workflow_exists,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_dataset_check_owner,
)
from fractal_server.app.routes.api.v2._aux_functions import _get_dataset_or_404
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_job_check_owner,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_project_check_owner,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_submitted_jobs_statement,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_check_owner,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_task_check_owner,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflowtask_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _verify_project_access,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)


async def test_404_functions(db):
    with pytest.raises(HTTPException, match="404"):
        await _get_workflowtask_or_404(workflowtask_id=9999, db=db)
    with pytest.raises(HTTPException, match="404"):
        await _get_workflow_or_404(workflow_id=9999, db=db)
    with pytest.raises(HTTPException, match="404"):
        await _get_dataset_or_404(dataset_id=9999, db=db)


async def test_get_project_check_owner(
    MockCurrentUser,
    project_factory_v2,
    db,
):
    async with MockCurrentUser() as other_user:
        other_project = await project_factory_v2(other_user)

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

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
    project_factory_v2,
    workflow_factory_v2,
    db,
):
    async with MockCurrentUser() as other_user:
        other_project = await project_factory_v2(other_user)
        other_workflow = await workflow_factory_v2(project_id=other_project.id)

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)

        # Test success
        await _get_workflow_check_owner(
            project_id=project.id,
            workflow_id=workflow.id,
            user_id=user.id,
            db=db,
        )
        assert workflow.project is not None
        assert len(workflow.project.user_list) > 0

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
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    workflowtask_factory_v2,
    db,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id, name="a")
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )
        other_workflow = await workflow_factory_v2(project_id=project.id)
        other_task = await task_factory_v2(user_id=user.id, name="B")
        other_wftask = await workflowtask_factory_v2(
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
    project_factory_v2,
    workflow_factory_v2,
    db,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)

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
    project_factory_v2,
    dataset_factory_v2,
    db,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        other_project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)

        # Test success
        res = await _get_dataset_check_owner(
            project_id=project.id,
            dataset_id=dataset.id,
            user_id=user.id,
            db=db,
        )
        dataset = res["dataset"]
        assert dataset.project is not None
        assert len(dataset.project.user_list) > 0

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
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    task_factory_v2,
    db,
    tmp_path,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user, id=1)
        other_project = await project_factory_v2(user, id=2)

        workflow = await workflow_factory_v2(project_id=project.id)
        t = await task_factory_v2(user_id=user.id)

        with pytest.raises(ValueError):
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=9999, db=db
            )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=t.id, db=db
        )

        dataset = await dataset_factory_v2(project_id=project.id)

        job = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
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


async def test_get_submitted_jobs_statement():
    stm = _get_submitted_jobs_statement()
    from sqlmodel.sql.expression import SelectOfScalar

    assert type(stm) is SelectOfScalar


async def test_verify_project_access(db):
    user1 = UserOAuth(email="x1@y.z", hashed_password="pass1")
    user2 = UserOAuth(email="x2@y.z", hashed_password="pass2")
    user3 = UserOAuth(email="x3@y.z", hashed_password="pass3")
    user4 = UserOAuth(email="x4@y.z", hashed_password="pass4")

    project1 = ProjectV2(name="p1")
    project2 = ProjectV2(name="p2")

    db.add_all([user1, user2, user3, user4, project1, project2])
    await db.commit()

    # USER1 is owner of both projects
    for project_id in [project1.id, project2.id]:
        db.add(
            LinkUserProjectV2(
                user_id=user1.id,
                project_id=project_id,
                is_owner=True,
                is_verified=True,
                can_write=True,
                can_execute=True,
            )
        )
    await db.commit()

    # Assertions
    for project_id in [project1.id, project2.id]:
        for access_type in ["read", "write", "execute"]:
            await _verify_project_access(
                project_id=project_id,
                user_id=user1.id,
                access_type=access_type,
                db=db,
            )

    # USER2:
    # - has read permission on project1
    # - is not verified on project2
    db.add(
        LinkUserProjectV2(
            user_id=user2.id,
            project_id=project1.id,
            is_owner=False,
            is_verified=True,
            can_write=False,
            can_execute=False,
        )
    )
    db.add(
        LinkUserProjectV2(
            user_id=user2.id,
            project_id=project2.id,
            is_owner=False,
            is_verified=False,
            can_write=False,
            can_execute=False,
        )
    )
    await db.commit()

    # Assertions
    await _verify_project_access(
        project_id=project1.id,
        user_id=user2.id,
        access_type="read",
        db=db,
    )
    for project_id, access_type in [
        (project1.id, "write"),
        (project1.id, "execute"),
        (project2.id, "read"),
        (project2.id, "write"),
        (project2.id, "execute"),
    ]:
        with pytest.raises(HTTPException):
            await _verify_project_access(
                project_id=project_id,
                user_id=user2.id,
                access_type=access_type,
                db=db,
            )

    # USER3:
    # - has write permission on project1
    # - has read permission on project2
    db.add(
        LinkUserProjectV2(
            user_id=user3.id,
            project_id=project1.id,
            is_owner=False,
            is_verified=True,
            can_write=True,
            can_execute=False,
        )
    )
    db.add(
        LinkUserProjectV2(
            user_id=user3.id,
            project_id=project2.id,
            is_owner=False,
            is_verified=True,
            can_write=False,
            can_execute=False,
        )
    )
    await db.commit()

    # Assertions
    for project_id, access_type in [
        (project1.id, "read"),
        (project1.id, "write"),
        (project2.id, "read"),
    ]:
        await _verify_project_access(
            project_id=project_id,
            user_id=user3.id,
            access_type=access_type,
            db=db,
        )
    for project_id, access_type in [
        (project1.id, "execute"),
        (project2.id, "write"),
        (project2.id, "execute"),
    ]:
        with pytest.raises(HTTPException):
            await _verify_project_access(
                project_id=project_id,
                user_id=user3.id,
                access_type=access_type,
                db=db,
            )

    # USER4:
    # - has execute permission on project1
    # - has write permission on project2
    db.add(
        LinkUserProjectV2(
            user_id=user4.id,
            project_id=project1.id,
            is_owner=False,
            is_verified=True,
            can_write=True,
            can_execute=True,
        )
    )
    db.add(
        LinkUserProjectV2(
            user_id=user4.id,
            project_id=project2.id,
            is_owner=False,
            is_verified=True,
            can_write=True,
            can_execute=False,
        )
    )
    await db.commit()

    # Assertions
    for project_id, access_type in [
        (project1.id, "read"),
        (project1.id, "write"),
        (project1.id, "execute"),
        (project2.id, "read"),
        (project2.id, "write"),
    ]:
        await _verify_project_access(
            project_id=project_id,
            user_id=user4.id,
            access_type=access_type,
            db=db,
        )
    with pytest.raises(HTTPException):
        await _verify_project_access(
            project_id=project2.id,
            user_id=user4.id,
            access_type="execute",
            db=db,
        )
