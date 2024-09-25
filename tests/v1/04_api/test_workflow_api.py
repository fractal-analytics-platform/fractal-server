import logging
from datetime import datetime
from datetime import timezone

from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.db import get_async_db
from fractal_server.app.models.v1 import Workflow
from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)


async def get_workflow(client, p_id, wf_id):
    res = await client.get(f"api/v1/project/{p_id}/workflow/{wf_id}/")
    assert res.status_code == 200
    return res.json()


async def add_task(client, index):
    t = dict(
        name=f"task{index}",
        source=f"source{index}",
        command="cmd",
        input_type="zarr",
        output_type="zarr",
    )
    res = await client.post("api/v1/task/", json=t)
    assert res.status_code == 201
    return res.json()


async def test_get_workflow(
    client, MockCurrentUser, project_factory, workflow_factory
):
    """
    GIVEN a Workflow in the db
    WHEN the endpoint to GET a Workflow by its id is called
    THEN the Workflow is returned
    """
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        p_id = project.id
        # Create workflow
        WORFKLOW_NAME = "My Workflow"
        wf = await workflow_factory(name=WORFKLOW_NAME, task_list=[])
        wf_id = wf.id
        # Get project (useful to check workflow.project relationship)
        res = await client.get(f"/api/v1/project/{p_id}/")
        assert res.status_code == 200
        EXPECTED_PROJECT = res.json()
        # Get workflow, and check relationship
        res = await client.get(f"/api/v1/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 200
        debug(res.json())
        assert res.json()["name"] == WORFKLOW_NAME
        assert res.json()["project"] == EXPECTED_PROJECT
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )

        # Get list of project workflows
        res = await client.get(f"/api/v1/project/{p_id}/workflow/")
        assert res.status_code == 200
        workflows = res.json()
        assert len(workflows) == 1
        assert workflows[0]["project"] == EXPECTED_PROJECT


async def test_get_user_workflows(
    client, MockCurrentUser, project_factory, workflow_factory, db
):
    """
    Test /api/v1/workflow/
    """

    async with MockCurrentUser(user_kwargs={}) as user:
        debug(user)

        project1 = await project_factory(user, name="p1")
        project2 = await project_factory(user, name="p2")
        await workflow_factory(project_id=project1.id, name="wf1a")
        await workflow_factory(project_id=project1.id, name="wf1b")
        await workflow_factory(project_id=project2.id, name="wf2a")

        res = await client.get("/api/v1/workflow/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 3
        assert set(wf["name"] for wf in res.json()) == {"wf1a", "wf1b", "wf2a"}


async def test_get_project_workflows(
    db, client, MockCurrentUser, project_factory, workflow_factory
):
    """
    GIVEN a Project containing three Workflows
    WHEN the endpoint to GET all the Workflows associated
        to that Project is called
    THEN the list of all its Workflows is returned
    """
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        other_project = await project_factory(user)
        workflow1 = {"name": "WF1"}
        workflow2 = {"name": "WF2"}
        workflow3 = {"name": "WF3"}
        workflow4 = {"name": "WF4"}
        await workflow_factory(**workflow1, project_id=project.id)
        await workflow_factory(**workflow2, project_id=project.id)
        await workflow_factory(**workflow3, project_id=other_project.id)
        await workflow_factory(**workflow4, project_id=project.id)

        res = await client.get(f"api/v1/project/{project.id}/workflow/")

        workflows = res.json()
        assert len(workflows) == 3
        assert len((await db.execute(select(Workflow))).scalars().all()) == 4


async def test_export_workflow_log(
    client,
    MockCurrentUser,
    task_factory,
    project_factory,
    workflow_factory,
    workflowtask_factory,
    db,
    caplog,
):
    """
    WHEN exporting a workflow with custom tasks
    THEN there must be a warning
    """

    # Create project and task
    async with MockCurrentUser() as user:
        TASK_OWNER = "someone"
        task = await task_factory(owner=TASK_OWNER, source="some-source")
        prj = await project_factory(user)
        wf = await workflow_factory(project_id=prj.id)
        async for this_db in get_async_db():
            await _workflow_insert_task(
                workflow_id=wf.id, task_id=task.id, db=this_db
            )

    # Export workflow
    caplog.clear()
    caplog.set_level(logging.WARNING)
    res = await client.get(
        f"/api/v1/project/{prj.id}/workflow/{wf.id}/export/"
    )
    assert res.status_code == 200
    debug(caplog.text)
    assert "not meant to be portable" in caplog.text
