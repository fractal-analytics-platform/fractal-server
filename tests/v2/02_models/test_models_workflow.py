import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


async def test_project_and_workflows(db):

    with pytest.raises(IntegrityError):
        # missing relatioship with project
        broken_workflow = WorkflowV2(name="broken")
        db.add(broken_workflow)
        await db.commit()
    await db.rollback()

    project = ProjectV2(name="project")

    # using `.project` relationship
    workflow1 = WorkflowV2(name="workflow1", project=project)
    db.add(project)
    db.add(workflow1)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(ProjectV2))
    db_project = project_query.scalars().one()
    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow1 = workflow_query.scalars().one()

    # test relationships
    assert db_workflow1.project_id == db_project.id
    assert db_workflow1.project.model_dump() == db_project.model_dump()
    # test defaults
    assert db_workflow1.task_list == []

    # using `.project_id` attribute
    workflow2 = WorkflowV2(name="workflow2", project_id=db_project.id)
    db.add(workflow2)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(ProjectV2))
    db_project = project_query.scalars().one()

    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow1, db_workflow2 = workflow_query.scalars().all()

    # test relationships
    assert db_workflow1.name == workflow1.name
    assert db_workflow2.name == workflow2.name
    assert db_workflow2.project_id == db_project.id
    assert db_workflow2.project.model_dump() == db_project.model_dump()

    # delete just one workflow
    await db.delete(db_workflow2)

    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow = workflow_query.scalars().one()
    assert db_workflow.name == workflow1.name

    # delete the project
    project_query = await db.execute(select(ProjectV2))
    db_project = project_query.scalars().one()
    await db.delete(db_project)

    DB_ENGINE = Inject(get_settings).DB_ENGINE
    if DB_ENGINE in ["postgres", "postgres-psycopg"]:
        with pytest.raises(IntegrityError):
            # WorkflowV2.project_id violates fk-contraint in Postgres
            await db.commit()
    else:
        # SQLite does not handle fk-constraints well
        await db.commit()
        db.expunge_all()

        project_query = await db.execute(select(ProjectV2))
        db_project = project_query.scalars().one_or_none()
        assert db_project is None

        workflow_query = await db.execute(select(WorkflowV2))
        db_workflow = workflow_query.scalars().one_or_none()
        assert db_workflow is not None  # no cascade
        assert db_workflow.project_id is not None  # fk is not null
        assert db_workflow.project is None  # relationship is null
