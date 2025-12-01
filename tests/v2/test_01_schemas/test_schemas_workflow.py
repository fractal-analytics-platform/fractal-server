import pytest
from pydantic import ValidationError

from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import WorkflowCreate
from fractal_server.app.schemas.v2 import WorkflowRead
from fractal_server.app.schemas.v2 import WorkflowTaskCreate
from fractal_server.app.schemas.v2 import WorkflowTaskUpdate
from fractal_server.app.schemas.v2 import WorkflowUpdate


async def test_schemas_workflow():
    project = ProjectV2(id=1, name="project")

    # Create

    workflow_create = WorkflowCreate(name="workflow")

    workflow = WorkflowV2(
        **workflow_create.model_dump(),
        id=1,
        project_id=project.id,
    )

    # Read

    WorkflowRead(
        **workflow.model_dump(),
        project=project.model_dump(),
        task_list=workflow.task_list,
    )

    # Update

    with pytest.raises(ValidationError):
        WorkflowUpdate(name=None)

    with pytest.raises(ValidationError):
        WorkflowUpdate(name="foo", reordered_workflowtask_ids=[1, 2, -3])

    WorkflowUpdate(name="new name", reordered_workflowtask_ids=[1, 2, 3])


async def test_schemas_workflow_task():
    for attribute in ("args_parallel", "args_non_parallel"):
        WorkflowTaskCreate(**{attribute: dict(something="else")})

        WorkflowTaskUpdate(**{attribute: dict(something="else")})

        WorkflowTaskCreate(**{attribute: None})

        WorkflowTaskUpdate(**{attribute: None})

        with pytest.raises(ValidationError) as e:
            WorkflowTaskUpdate(**{attribute: dict(zarr_url="/something")})
        assert "contains the following forbidden keys" in str(e.value)

        with pytest.raises(ValidationError) as e:
            WorkflowTaskCreate(**{attribute: dict(zarr_url="/something")})
        assert "contains the following forbidden keys" in str(e.value)
