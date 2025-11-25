import pytest
from fastapi import HTTPException

from fractal_server.app.models.linkuserproject import LinkUserProjectV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _check_workflow_exists,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_dataset_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import _get_dataset_or_404
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_job_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_project_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_submitted_jobs_statement,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import _get_workflow_or_404
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflow_task_check_access,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _get_workflowtask_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2.sharing import ProjectPermissions


async def test_404_functions(db):
    with pytest.raises(HTTPException, match="404"):
        await _get_workflowtask_or_404(workflowtask_id=9999, db=db)
    with pytest.raises(HTTPException, match="404"):
        await _get_workflow_or_404(workflow_id=9999, db=db)
    with pytest.raises(HTTPException, match="404"):
        await _get_dataset_or_404(dataset_id=9999, db=db)


async def test_get_project_check_access(
    MockCurrentUser,
    project_factory_v2,
    db,
):
    async with MockCurrentUser() as other_user:
        other_project = await project_factory_v2(other_user)

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

        # Test success
        await _get_project_check_access(
            project_id=project.id,
            user_id=user.id,
            required_permissions=ProjectPermissions.EXECUTE,
            db=db,
        )

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_project_check_access(
                project_id=project.id + 1,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Project not found"

        # Test fail 2: link_user_project is None
        with pytest.raises(HTTPException) as err:
            await _get_project_check_access(
                project_id=other_project.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 403
        assert (
            "You are not authorized to perform this action." in err.value.detail
        )

    async with MockCurrentUser() as read_only_user:
        # Read only user for `project`
        db.add(
            LinkUserProjectV2(
                project_id=project.id,
                user_id=read_only_user.id,
                is_owner=False,
                is_verified=True,
                permissions=ProjectPermissions.READ,
            )
        )
        await db.commit()

        # test `required_permissions not in link_user_project.permissions`
        await _get_project_check_access(
            project_id=project.id,
            user_id=read_only_user.id,
            required_permissions=ProjectPermissions.READ,  # <---
            db=db,
        )
        for required_permissions in [
            ProjectPermissions.WRITE,
            ProjectPermissions.EXECUTE,
        ]:
            with pytest.raises(HTTPException) as err:
                await _get_project_check_access(
                    project_id=project.id,
                    user_id=read_only_user.id,
                    required_permissions=required_permissions,
                    db=db,
                )
            assert err.value.status_code == 403
            assert (
                "You are not authorized to perform this action."
                in err.value.detail
            )

    async with MockCurrentUser() as unverified_user:
        db.add(
            LinkUserProjectV2(
                project_id=project.id,
                user_id=unverified_user.id,
                is_owner=False,
                is_verified=False,  # <---
                permissions=ProjectPermissions.EXECUTE,
            )
        )
        await db.commit()

        # test `not link_user_project.is_verified`
        for required_permissions in ProjectPermissions:
            with pytest.raises(HTTPException) as err:
                await _get_project_check_access(
                    project_id=project.id,
                    user_id=unverified_user.id,
                    required_permissions=required_permissions,
                    db=db,
                )
            assert err.value.status_code == 403
            assert (
                "You are not authorized to perform this action."
                in err.value.detail
            )


async def test_get_workflow_check_access(
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
        await _get_workflow_check_access(
            project_id=project.id,
            workflow_id=workflow.id,
            user_id=user.id,
            required_permissions=ProjectPermissions.EXECUTE,
            db=db,
        )
        assert workflow.project is not None

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_workflow_check_access(
                project_id=project.id,
                workflow_id=workflow.id + 1,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Workflow not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_workflow_check_access(
                project_id=project.id,
                workflow_id=other_workflow.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Workflow not found"


async def test_get_workflow_task_check_access(
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
        await _get_workflow_task_check_access(
            project_id=project.id,
            workflow_id=workflow.id,
            workflow_task_id=wftask.id,
            user_id=user.id,
            required_permissions=ProjectPermissions.EXECUTE,
            db=db,
        )
        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_workflow_task_check_access(
                project_id=project.id,
                workflow_id=workflow.id,
                workflow_task_id=wftask.id + other_wftask.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "WorkflowTask not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_workflow_task_check_access(
                project_id=project.id,
                workflow_id=workflow.id,
                workflow_task_id=other_wftask.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "WorkflowTask not found"


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


async def test_get_dataset_check_access(
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
        res = await _get_dataset_check_access(
            project_id=project.id,
            dataset_id=dataset.id,
            user_id=user.id,
            required_permissions=ProjectPermissions.EXECUTE,
            db=db,
        )
        dataset = res["dataset"]
        assert dataset.project is not None

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_dataset_check_access(
                project_id=project.id,
                dataset_id=dataset.id + 1,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Dataset not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_dataset_check_access(
                project_id=other_project.id,
                dataset_id=dataset.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Dataset not found"


async def test_get_job_check_access(
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
        await _get_job_check_access(
            project_id=project.id,
            job_id=job.id,
            user_id=user.id,
            required_permissions=ProjectPermissions.EXECUTE,
            db=db,
        )

        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_job_check_access(
                project_id=project.id,
                job_id=job.id + 1,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Job not found"

        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_job_check_access(
                project_id=other_project.id,
                job_id=job.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Job not found"


async def test_get_submitted_jobs_statement():
    stm = _get_submitted_jobs_statement()
    from sqlmodel.sql.expression import SelectOfScalar

    assert type(stm) is SelectOfScalar
