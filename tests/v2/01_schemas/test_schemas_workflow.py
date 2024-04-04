from devtools import debug

from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowReadV2
from fractal_server.app.schemas.v2 import WorkflowUpdateV2


async def test_schemas_workflow_v2(db):

    project = ProjectV2(name="project")
    debug(project)
    db.add(project)
    await db.commit()

    # Create

    workflow_create = WorkflowCreateV2(name="workflow")
    debug(workflow_create)

    workflow = WorkflowV2(**workflow_create.dict(), project_id=project.id)
    debug(workflow)

    db.add(workflow)
    await db.commit()

    # Read

    await db.refresh(workflow)
    workflow_read = WorkflowReadV2(
        **workflow.model_dump(),
        project=workflow.project,
        task_list=workflow.task_list,
    )
    debug(workflow_read)

    # Update

    workflow_update = WorkflowUpdateV2(name="new name")
    debug(workflow_update)

    for key, value in workflow_update.dict(exclude_unset=True).items():
        setattr(workflow, key, value)

    await db.commit()
    assert workflow.name == "new name"
    debug(workflow)
