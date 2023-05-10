import pytest
from devtools import debug

from fastapi import HTTPException

from fractal_server.app.api.v1._aux_functions import _get_project_check_owner
from fractal_server.app.api.v1._aux_functions import _get_workflow_check_owner
from fractal_server.app.api.v1._aux_functions import _get_workflow_task_check_owner
from fractal_server.app.api.v1._aux_functions import _check_workflow_exists
from fractal_server.app.api.v1._aux_functions import _get_dataset_check_owner
from fractal_server.app.api.v1._aux_functions import _get_job_check_owner

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
            project_id=project.id, user_id = user.id, db=db
        )
        # Test fail 1
        with pytest.raises(HTTPException) as err:
            await _get_project_check_owner(
                project_id=project.id+1, user_id = user.id, db=db
            )
        assert err.value.status_code == 404
        assert err.value.detail == "Project not found"
        # Test fail 2
        with pytest.raises(HTTPException) as err:
            await _get_project_check_owner(
                project_id=other_project.id, user_id = user.id, db=db
            )
        assert err.value.status_code == 403
        assert err.value.detail == f"Not allowed on project {other_project.id}"





async def test_get_workflow_check_owner():
    pass

async def test_get_workflow_task_check_owner():
    pass

async def test_check_workflow_exists():
    pass

async def test_get_dataset_check_owner():
    pass

async def test_get_job_check_owner():
    pass
