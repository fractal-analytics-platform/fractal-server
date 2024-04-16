import pytest
from pydantic import ValidationError

from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowReadV2
from fractal_server.app.schemas.v2 import WorkflowUpdateV2


async def test_schemas_workflow_v2():

    project = ProjectV2(id=1, name="project")

    # Create

    workflow_create = WorkflowCreateV2(name="workflow")

    workflow = WorkflowV2(
        **workflow_create.dict(),
        id=1,
        project_id=project.id,
    )

    # Read

    WorkflowReadV2(
        **workflow.model_dump(),
        project=project,
        task_list=workflow.task_list,
    )

    # Update

    with pytest.raises(ValidationError):
        WorkflowUpdateV2(name=None)

    workflow_update = WorkflowUpdateV2(name="new name")

    for key, value in workflow_update.dict(exclude_unset=True).items():
        setattr(workflow, key, value)

    assert workflow.name == "new name"
