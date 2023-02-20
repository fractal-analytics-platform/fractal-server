import json

from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models import TaskRead
from fractal_server.app.models import Workflow
from fractal_server.app.models import WorkflowExport
from fractal_server.app.models import WorkflowImport
from fractal_server.app.models import WorkflowRead
from fractal_server.app.models import WorkflowTask


async def test_post_workflow(db, client, MockCurrentUser, project_factory):
    async with MockCurrentUser(persist=True) as user:
        res = await client.post(
            "api/v1/workflow/",
            json={"name": "My Workflow"},
        )
        assert res.status_code == 422  # no project_id

        project1 = await project_factory(user)
        p1_id = project1.id
        workflow1 = {
            "name": "My Workflow",
            "project_id": p1_id,
        }
        project2 = await project_factory(user)
        p2_id = project2.id
        workflow2 = {
            "name": "My Workflow",
            "project_id": p2_id,
        }

        res = await client.post(
            "api/v1/workflow/",
            json=workflow1,
        )
        assert res.status_code == 201

        res = await client.post(
            "api/v1/workflow/",
            json=workflow1,
        )
        assert res.status_code == 422  # already in use

        res = await client.post(
            "api/v1/workflow/",
            json={
                "name": "My Workflow",
                "project_id": p1_id + p2_id,
            },
        )
        assert res.status_code == 404  # project does not exist

        res = await client.post(
            "api/v1/workflow/",
            json=workflow2,
        )
        assert res.status_code == 201  # same name, different projects

        for id in [p1_id, p2_id]:
            stm = select(Workflow).where(Workflow.project_id == id)
            _workflow = await db.execute(stm)
            db_workflow = _workflow.scalars().one()

            assert db_workflow.name == "My Workflow"
            assert db_workflow.project_id == id


async def test_delete_workflow(
    db, client, MockCurrentUser, project_factory, collect_packages
):
    """
    GIVEN a Workflow with two Tasks
    WHEN the endpoint that deletes a Workflow is called
    THEN the Workflow and its associated WorkflowTasks are removed from the db
    """
    async with MockCurrentUser(persist=True) as user:

        # Create project
        project = await project_factory(user)
        p_id = project.id
        workflow = {
            "name": "My Workflow",
            "project_id": p_id,
        }

        # Create workflow
        res = await client.post(
            "api/v1/workflow/",
            json=workflow,
        )
        wf_id = res.json()["id"]

        # Add a dummy task to workflow
        res = await client.post(
            f"api/v1/workflow/{wf_id}/add-task/",
            json=dict(task_id=collect_packages[0].id),
        )
        assert res.status_code == 201
        debug(res.json())

        # Verify that the WorkflowTask was correctly inserted into the Workflow
        stm = (
            select(WorkflowTask)
            .join(Workflow)
            .where(WorkflowTask.workflow_id == wf_id)
        )
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 1

        # Delete the Workflow
        res = await client.delete(f"api/v1/workflow/{wf_id}")
        assert res.status_code == 204

        # Check that the Workflow was deleted
        assert not await db.get(Workflow, wf_id)

        # Check that the WorkflowTask was deleted
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 0


async def test_get_workflow(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Workflow in the db
    WHEN the endpoint to GET a Workflow by its id is called
    THEN the Workflow is returned
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        p_id = project.id
        workflow = {
            "name": "My Workflow",
            "project_id": p_id,
            "task_list": [],
        }
        res = await client.post(
            "api/v1/workflow/",
            json=workflow,
        )
        wf_id = res.json()["id"]
        res = await client.get(f"/api/v1/workflow/{wf_id}")

        assert res.status_code == 200
        workflow["id"] = wf_id
        assert res.json() == workflow


async def test_post_newtask(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to POST a new WorkflowTask inside
        the Workflow.task_list is called
    THEN the new WorkflowTask is inserted in Workflow.task_list
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {
            "name": "My Workflow",
            "project_id": project.id,
        }
        res = await client.post(
            "api/v1/workflow/",
            json=workflow,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        workflow = await db.get(Workflow, wf_id)
        t0 = await task_factory()
        t1 = await task_factory()
        await workflow.insert_task(t0.id, db=db)
        await workflow.insert_task(t1.id, db=db)
        await db.refresh(workflow)

        assert len(workflow.task_list) == 2
        assert workflow.task_list[0].task == t0
        assert workflow.task_list[1].task == t1

        await db.refresh(workflow)

        t2 = await task_factory()
        last_task = {"task_id": t2.id, "args": {"a": 0, "b": 1}}

        res = await client.post(
            f"api/v1/workflow/{wf_id}/add-task/",
            json=last_task,
        )
        assert res.status_code == 201

        t0b = await task_factory()
        second_task = {
            "task_id": t0b.id,
            "order": 1,
        }
        res = await client.post(
            f"api/v1/workflow/{wf_id}/add-task/",
            json=second_task,
        )
        assert res.status_code == 201

        # Get back workflow
        res = await client.get(f"api/v1/workflow/{wf_id}")
        assert res.status_code == 200
        workflow = WorkflowRead(**res.json())
        debug(workflow)

        assert len(workflow.task_list) == 4
        assert workflow.task_list[0].task == TaskRead(**t0.dict())
        assert workflow.task_list[1].task == TaskRead(**t0b.dict())
        assert workflow.task_list[2].task == TaskRead(**t1.dict())
        assert workflow.task_list[3].task == TaskRead(**t2.dict())
        assert workflow.task_list[3].args == last_task["args"]


async def test_delete_workflow_task(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to DELETE a WorkflowTask in the
        Workflow.task_list is called
    THEN the selected WorkflowTask is properly removed
        from Workflow.task_list
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {
            "name": "My Workflow",
            "project_id": project.id,
        }
        res = await client.post(
            "api/v1/workflow/",
            json=workflow,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        workflow = await db.get(Workflow, wf_id)
        t0 = await task_factory()
        t1 = await task_factory()
        t2 = await task_factory()

        await workflow.insert_task(t0.id, db=db)
        await workflow.insert_task(t1.id, db=db)
        await workflow.insert_task(t2.id, db=db)
        await db.refresh(workflow)

        assert (
            len((await db.execute(select(WorkflowTask))).scalars().all()) == 3
        )
        assert len(workflow.task_list) == 3
        for i, task in enumerate(workflow.task_list):
            assert task.order == i

        res = await client.delete(f"api/v1/workflow/{wf_id}/rm-task/{t1.id}")
        assert res.status_code == 204

        await db.refresh(workflow)
        assert (
            len((await db.execute(select(WorkflowTask))).scalars().all()) == 2
        )
        assert len(workflow.task_list) == 2
        for i, task in enumerate(workflow.task_list):
            assert task.order == i


async def test_get_project_workflows(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Project containing three Workflows
    WHEN the endpoint to GET all the Workflows associated
        to that Project is called
    THEN the list of all its Workflows is returned
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        other_project = await project_factory(user)
        workflow1 = {"name": "WF1", "project_id": project.id}
        workflow2 = {"name": "WF2", "project_id": project.id}
        workflow3 = {"name": "WF3", "project_id": other_project.id}
        workflow4 = {"name": "WF4", "project_id": project.id}
        res = await client.post("api/v1/workflow/", json=workflow1)
        res = await client.post("api/v1/workflow/", json=workflow2)
        res = await client.post("api/v1/workflow/", json=workflow3)
        res = await client.post("api/v1/workflow/", json=workflow4)

        res = await client.get(f"api/v1/project/{project.id}/workflows/")

        workflow_list = res.json()
        assert len(workflow_list) == 3
        assert len((await db.execute(select(Workflow))).scalars().all()) == 4


async def test_patch_workflow(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Workflow
    WHEN the endpoint to PATCH a Workflow is called
    THEN the Workflow is updated
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {"name": "WF", "project_id": project.id}
        res = await client.post("api/v1/workflow/", json=workflow)
        wf_id = res.json()["id"]
        assert res.json()["name"] == "WF"

        workflow = await db.get(Workflow, wf_id)
        res = await client.get(f"api/v1/project/{project.id}/workflows/")
        assert len(res.json()) == 1

        patch = {"name": "new_WF"}
        res = await client.patch(f"api/v1/workflow/{wf_id}", json=patch)

        await db.refresh(workflow)
        assert workflow.name == "new_WF"
        res = await client.get(f"api/v1/project/{project.id}/workflows/")
        assert len(res.json()) == 1


async def test_patch_workflow_task(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called
    THEN the WorkflowTask is updated
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {"name": "WF", "project_id": project.id}
        res = await client.post("api/v1/workflow/", json=workflow)
        wf_id = res.json()["id"]

        workflow = await db.get(Workflow, wf_id)
        t0 = await task_factory()
        await workflow.insert_task(t0.id, db=db)
        await db.refresh(workflow)
        assert workflow.task_list[0].args is None

        payload = dict(args={"a": 123, "d": 321}, meta={"executor": "cpu-low"})
        res = await client.patch(
            f"api/v1/workflow/{workflow.id}/"
            f"edit-task/{workflow.task_list[0].id}",
            json=payload,
        )

        patched_workflow_task = res.json()
        debug(patched_workflow_task)
        assert patched_workflow_task["args"] == payload["args"]
        assert patched_workflow_task["meta"] == payload["meta"]
        assert res.status_code == 200

        payload_up = dict(args={"a": {"c": 43}, "b": 123})
        res = await client.patch(
            f"api/v1/workflow/{workflow.id}/"
            f"edit-task/{workflow.task_list[0].id}",
            json=payload_up,
        )
        patched_workflow_task_up = res.json()
        assert patched_workflow_task_up["args"] == dict(
            a=dict(c=43), b=123, d=321
        )


async def test_patch_workflow_task_failures(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called with invalid arguments
    THEN the correct status code is returned
    """
    async with MockCurrentUser(persist=True) as user:

        # Prepare two workflows, with one task each
        project = await project_factory(user)
        workflow1 = {"name": "WF1", "project_id": project.id}
        res = await client.post("api/v1/workflow/", json=workflow1)
        wf1_id = res.json()["id"]
        workflow2 = {"name": "WF2", "project_id": project.id}
        res = await client.post("api/v1/workflow/", json=workflow2)
        wf2_id = res.json()["id"]
        workflow1 = await db.get(Workflow, wf1_id)
        workflow2 = await db.get(Workflow, wf2_id)
        task1 = await task_factory()
        task2 = await task_factory()
        assert task1.id != task2.id
        await workflow1.insert_task(task1.id, db=db)
        await workflow2.insert_task(task2.id, db=db)
        await db.refresh(workflow1)
        await db.refresh(workflow2)
        workflow_task_1 = workflow1.task_list[0]
        workflow_task_2 = workflow2.task_list[0]

        # Modify parallelization_level
        payload = dict(meta={"parallelization_level": "XXX"})
        res = await client.patch(
            f"api/v1/workflow/{workflow1.id}/"
            f"edit-task/{workflow_task_1.id}",
            json=payload,
        )
        assert res.status_code == 422

        # Edit a WorkflowTask for a missing Workflow
        WORKFLOW_ID = 999
        WORKFLOW_TASK_ID = 1
        res = await client.patch(
            f"api/v1/workflow/{WORKFLOW_ID}/edit-task/{WORKFLOW_TASK_ID}",
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 404

        # Edit a missing WorkflowTask
        WORKFLOW_ID = 1
        WORKFLOW_TASK_ID = 999
        res = await client.patch(
            f"api/v1/workflow/{WORKFLOW_ID}/edit-task/{WORKFLOW_TASK_ID}",
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 404

        # Edit a valid WorkflowTask without specifying the right Workflow
        WORKFLOW_ID = workflow1.id
        WORKFLOW_TASK_ID = workflow_task_2.id
        res = await client.patch(
            f"api/v1/workflow/{WORKFLOW_ID}/edit-task/{WORKFLOW_TASK_ID}",
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 422


async def test_import_export_workflow(
    client,
    MockCurrentUser,
    project_factory,
    testdata_path,
    collect_packages,
):

    # Load workflow to be imported into DB
    with (testdata_path / "import_export/workflow.json").open("r") as f:
        workflow_from_file = json.load(f)

    # Create project
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

    # Import workflow into project
    payload = WorkflowImport(**workflow_from_file).dict(exclude_none=True)
    res = await client.post(
        f"/api/v1/project/{prj.id}/import-workflow/", json=payload
    )
    debug(res)
    assert res.status_code == 201
    workflow_imported = res.json()
    debug(workflow_imported)

    # Check that output can be cast to WorkflowRead
    WorkflowRead(**workflow_imported)

    # Export workflow
    workflow_id = workflow_imported["id"]
    res = await client.get(f"/api/v1/workflow/{workflow_id}/export/")
    debug(res.status_code)
    workflow_exported = res.json()
    debug(workflow_exported)
    assert "id" not in workflow_exported
    assert "project_id" not in workflow_exported
    for wftask in workflow_exported["task_list"]:
        assert "id" not in wftask
        assert "task_id" not in wftask
        assert "workflow_id" not in wftask
        assert "id" not in wftask["task"]
    assert res.status_code == 200

    # Check that output can be cast to WorkflowExport
    WorkflowExport(**workflow_exported)

    # Before cheching that the exported workflow matches with the one in the
    # original JSON file, we need to update the Workflow.task_list.Task.command
    # attributes, since they depend on the server folders.
    wf_old = WorkflowExport(**workflow_from_file).dict(exclude_none=True)
    wf_new = WorkflowExport(**workflow_exported).dict(exclude_none=True)
    path_old = "/SOME/PATH/"
    path_new = collect_packages[0].command.split("dummy0")[0]
    for ind, wf_task in enumerate(wf_old["task_list"]):
        new_command = wf_task["task"]["command"].replace(path_old, path_new)
        wf_old["task_list"][ind]["task"]["command"] = new_command

    # Check that the exported workflow matches with the one in the original
    # JSON file
    assert wf_old == wf_new
