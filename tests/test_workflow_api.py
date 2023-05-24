import json

import pytest
from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models import Workflow
from fractal_server.app.models import WorkflowExport
from fractal_server.app.models import WorkflowImport
from fractal_server.app.models import WorkflowRead
from fractal_server.app.models import WorkflowTask


async def get_workflow(client, p_id, wf_id):
    res = await client.get(f"api/v1/project/{p_id}/workflow/{wf_id}")
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


async def test_post_workflow(db, client, MockCurrentUser, project_factory):
    async with MockCurrentUser(persist=True) as user:
        project_id = None
        res = await client.post(
            f"api/v1/project/{project_id}/workflow/",
            json={"name": "My Workflow"},
        )
        assert res.status_code == 422  # no project_id

        project1 = await project_factory(user)
        p1_id = project1.id
        workflow1 = {"name": "My Workflow"}
        project2 = await project_factory(user)
        p2_id = project2.id
        workflow2 = {"name": "My Workflow"}

        res = await client.post(
            f"api/v1/project/{p1_id}/workflow/",
            json=workflow1,
        )
        assert res.status_code == 201

        res = await client.post(
            f"api/v1/project/{p1_id}/workflow/",
            json=workflow1,
        )
        assert res.status_code == 422  # already in use

        res = await client.post(
            f"api/v1/project/{p1_id + p2_id}/workflow/",
            json={"name": "My Workflow"},
        )
        assert res.status_code == 404  # project does not exist

        res = await client.post(
            f"api/v1/project/{p2_id}/workflow/",
            json=workflow2,
        )
        assert res.status_code == 201  # same name, different projects

        for _id in [p1_id, p2_id]:
            stm = select(Workflow).where(Workflow.project_id == _id)
            _workflow = await db.execute(stm)
            db_workflow = _workflow.scalars().one()

            assert db_workflow.name == "My Workflow"
            assert db_workflow.project_id == _id


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
        }

        # Create workflow
        res = await client.post(
            f"api/v1/project/{p_id}/workflow/",
            json=workflow,
        )
        wf_id = res.json()["id"]

        # Add a dummy task to workflow
        res = await client.post(
            (
                f"api/v1/project/{p_id}/workflow/{wf_id}/wftask/?"
                f"task_id={collect_packages[0].id}"
            ),
            json={},
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
        res = await client.delete(f"api/v1/project/{p_id}/workflow/{wf_id}")
        assert res.status_code == 204

        # Check that the Workflow was deleted
        res = await client.get(f"api/v1/project/{p_id}/workflow/{wf_id}")
        assert res.status_code == 404

        # Check that the WorkflowTask was deleted
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 0


async def test_get_workflow(client, MockCurrentUser, project_factory):
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
            "task_list": [],
        }
        res = await client.post(
            f"api/v1/project/{p_id}/workflow/",
            json=workflow,
        )
        wf_id = res.json()["id"]
        res = await client.get(f"/api/v1/project/{p_id}/workflow/{wf_id}")

        assert res.status_code == 200
        workflow.update({"id": wf_id, "project_id": p_id})
        assert res.json() == workflow


async def test_post_worfkflow_task(client, MockCurrentUser, project_factory):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to POST a new WorkflowTask inside
        the Workflow.task_list is called
    THEN the new WorkflowTask is inserted in Workflow.task_list
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {"name": "My Workflow"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/",
            json=workflow,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        for index in range(2):
            t = await add_task(client, index)
            res = await client.post(
                (
                    f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                    f"?task_id={t['id']}"
                ),
                json={},
            )
            workflow = await get_workflow(client, project.id, wf_id)
            assert len(workflow["task_list"]) == index + 1
            assert workflow["task_list"][-1]["task"] == t

        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 2

        t2 = await add_task(client, 2)
        args_payload = dict(args={"a": 0, "b": 1})
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                f"?task_id={t2['id']}"
            ),
            json=args_payload,
        )
        assert res.status_code == 201

        t0b = await add_task(client, "0b")
        payload = dict(order=1)
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                f"?task_id={t0b['id']}"
            ),
            json=payload,
        )
        assert res.status_code == 201

        # Get back workflow
        workflow = await get_workflow(client, project.id, wf_id)
        task_list = workflow["task_list"]
        assert len(task_list) == 4
        assert task_list[0]["task"]["name"] == "task0"
        assert task_list[1]["task"]["name"] == "task0b"
        assert task_list[2]["task"]["name"] == "task1"
        assert task_list[3]["task"]["name"] == "task2"
        assert task_list[3]["args"] == args_payload["args"]


async def test_delete_workflow_task(
    db, client, MockCurrentUser, project_factory
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
        workflow = {"name": "My Workflow"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/",
            json=workflow,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        workflow = await get_workflow(client, project.id, wf_id)
        t0 = await add_task(client, 0)
        t1 = await add_task(client, 1)
        t2 = await add_task(client, 2)

        wftasks = []
        for t in [t0, t1, t2]:
            res = await client.post(
                (
                    f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                    f"?task_id={t['id']}"
                ),
                json={},
            )
            assert res.status_code == 201
            wftasks.append(res.json())

        assert (
            len((await db.execute(select(WorkflowTask))).scalars().all()) == 3
        )
        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 3
        for i, task in enumerate(workflow["task_list"]):
            assert task["order"] == i

        # Remove the WorkflowTask in the middle
        wf_task_id = wftasks[1]["id"]
        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/{wf_task_id}"
        )
        assert res.status_code == 204

        workflow = await get_workflow(client, project.id, wf_id)
        assert (
            len((await db.execute(select(WorkflowTask))).scalars().all()) == 2
        )
        assert len(workflow["task_list"]) == 2
        for i, task in enumerate(workflow["task_list"]):
            assert task["order"] == i


async def test_get_project_workflows(
    db, client, MockCurrentUser, project_factory
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
        workflow1 = {"name": "WF1"}
        workflow2 = {"name": "WF2"}
        workflow3 = {"name": "WF3"}
        workflow4 = {"name": "WF4"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow1
        )
        assert res.status_code == 201
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow2
        )
        assert res.status_code == 201
        res = await client.post(
            f"api/v1/project/{other_project.id}/workflow/", json=workflow3
        )
        assert res.status_code == 201
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow4
        )
        assert res.status_code == 201

        res = await client.get(f"api/v1/project/{project.id}/workflow/")

        workflow_list = res.json()
        assert len(workflow_list) == 3
        assert len((await db.execute(select(Workflow))).scalars().all()) == 4


async def test_patch_workflow(client, MockCurrentUser, project_factory):
    """
    GIVEN a Workflow
    WHEN the endpoint to PATCH a Workflow is called
    THEN the Workflow is updated
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow
        )
        wf_id = res.json()["id"]
        assert res.json()["name"] == "WF"

        res = await client.get(f"api/v1/project/{project.id}/workflow/")
        assert len(res.json()) == 1

        patch = {"name": "new_WF"}
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}", json=patch
        )

        new_workflow = await get_workflow(client, project.id, wf_id)
        assert new_workflow["name"] == "new_WF"

        res = await client.get(f"api/v1/project/{project.id}/workflow/")
        assert len(res.json()) == 1


async def test_patch_workflow_task(client, MockCurrentUser, project_factory):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called
    THEN the WorkflowTask is updated
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        t = await add_task(client, 0)
        payload = {"task_id": t["id"]}
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                f"?task_id={t['id']}"
            ),
            json={},
        )
        assert res.status_code == 201

        workflow = await get_workflow(client, project.id, wf_id)

        assert workflow["task_list"][0]["args"] is None

        payload = dict(args={"a": 123, "d": 321}, meta={"executor": "cpu-low"})
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}",
            json=payload,
        )

        patched_workflow_task = res.json()
        debug(patched_workflow_task)
        assert patched_workflow_task["args"] == payload["args"]
        assert patched_workflow_task["meta"] == payload["meta"]
        assert res.status_code == 200

        payload_up = dict(args={"a": {"c": 43}, "b": 123})
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}",
            json=payload_up,
        )
        patched_workflow_task_up = res.json()
        assert patched_workflow_task_up["args"] == dict(
            a=dict(c=43), b=123, d=321
        )


async def test_patch_workflow_task_failures(
    client, MockCurrentUser, project_factory
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called with invalid arguments
    THEN the correct status code is returned
    """
    async with MockCurrentUser(persist=True) as user:

        # Prepare two workflows, with one task each
        project = await project_factory(user)
        workflow1 = {"name": "WF1"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow1
        )
        wf1_id = res.json()["id"]
        workflow2 = {"name": "WF2"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow2
        )
        wf2_id = res.json()["id"]

        t1 = await add_task(client, 1)
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf1_id}/wftask/"
                f"?task_id={t1['id']}"
            ),
            json={},
        )

        t2 = await add_task(client, 2)

        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf2_id}/wftask/"
                f"?task_id={t2['id']}"
            ),
            json={},
        )

        workflow1 = await get_workflow(client, project.id, wf1_id)
        workflow2 = await get_workflow(client, project.id, wf2_id)
        workflow_task_1 = workflow1["task_list"][0]
        workflow_task_2 = workflow2["task_list"][0]

        # Modify parallelization_level
        payload = dict(meta={"parallelization_level": "XXX"})
        res = await client.patch(
            (
                f"api/v1/project/{project.id}/workflow/{workflow1['id']}/"
                f"wftask/{workflow_task_1['id']}"
            ),
            json=payload,
        )
        assert res.status_code == 422

        # Edit a WorkflowTask for a missing Workflow
        WORKFLOW_ID = 999
        WORKFLOW_TASK_ID = 1
        res = await client.patch(
            (
                f"api/v1/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}"
            ),
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 404

        # Edit a missing WorkflowTask
        WORKFLOW_ID = 1
        WORKFLOW_TASK_ID = 999
        res = await client.patch(
            (
                f"api/v1/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}"
            ),
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 404

        # Edit a valid WorkflowTask without specifying the right Workflow
        WORKFLOW_ID = workflow1["id"]
        WORKFLOW_TASK_ID = workflow_task_2["id"]
        res = await client.patch(
            (
                f"api/v1/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}"
            ),
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
    # Modify tasks' source to match the existing one
    existing_source = collect_packages[0].source
    debug(existing_source)
    for ind, wftask in enumerate(workflow_from_file["task_list"]):
        workflow_from_file["task_list"][ind]["task"][
            "source"
        ] = existing_source  # noqa
    debug(workflow_from_file)

    # Create project
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

    # Import workflow into project
    payload = WorkflowImport(**workflow_from_file).dict(exclude_none=True)
    debug(payload)
    res = await client.post(
        f"/api/v1/project/{prj.id}/workflow/import/", json=payload
    )
    debug(res.json())
    assert res.status_code == 201
    workflow_imported = res.json()
    debug(workflow_imported)

    # Check that output can be cast to WorkflowRead
    WorkflowRead(**workflow_imported)

    # Export workflow
    workflow_id = workflow_imported["id"]
    res = await client.get(
        f"/api/v1/project/{prj.id}/workflow/{workflow_id}/export/"
    )
    debug(res)
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

    # Check that the exported workflow matches with the one in the original
    # JSON file
    wf_old = WorkflowExport(**workflow_from_file).dict(exclude_none=True)
    wf_new = WorkflowExport(**workflow_exported).dict(exclude_none=True)

    assert wf_old == wf_new


async def test_import_export_workflow_fail(
    client,
    MockCurrentUser,
    project_factory,
    testdata_path,
    task_factory,
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

    await task_factory(name="valid", source="test_source")
    payload = {
        "name": "MyWorkflow",
        "task_list": [
            {"order": 0, "task": {"name": "dummy", "source": "xyz"}}
        ],
    }
    res = await client.post(
        f"/api/v1/project/{prj.id}/workflow/import/", json=payload
    )
    assert res.status_code == 422
    assert "Found 0 tasks with source" in res.json()["detail"]

    payload = {
        "name": "MyWorkflow",
        "task_list": [
            {"order": 0, "task": {"name": "invalid", "source": "test_source"}}
        ],
    }
    res = await client.post(
        f"/api/v1/project/{prj.id}/workflow/import/", json=payload
    )
    assert res.status_code == 422
    assert "Found 0 tasks with name" in res.json()["detail"]


reorder_cases = []
reorder_cases.append([1, 2])
reorder_cases.append([2, 1])
reorder_cases.append([1, 2, 3])
reorder_cases.append([1, 3, 2])
reorder_cases.append([2, 1, 3])
reorder_cases.append([2, 3, 1])
reorder_cases.append([3, 2, 1])
reorder_cases.append([3, 1, 2])
reorder_cases.append([4, 3, 5, 6, 1, 2])


@pytest.mark.parametrize("reordered_workflowtask_ids", reorder_cases)
async def test_reorder_task_list(
    reordered_workflowtask_ids,
    client,
    MockCurrentUser,
    project_factory,
):
    """
    GIVEN a workflow with a task_list
    WHEN we call its PATCH endpoint with the order_permutation attribute
    THEN the task_list is reodered correctly
    """

    num_tasks = len(reordered_workflowtask_ids)

    async with MockCurrentUser(persist=True) as user:

        # Create project and empty workflow
        project = await project_factory(user)
        payload = {"name": "WF"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=payload
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        # Make no-op API call to reorder an empty task list
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}",
            json=dict(reordered_workflowtask_ids=[]),
        )
        assert res.status_code == 200

        # Create tasks and insert WorkflowTasks
        for i in range(num_tasks):
            t = await add_task(client, i)
            res = await client.post(
                (
                    f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                    f"?task_id={t['id']}"
                ),
                json={},
            )

        # At this point, all WorkflowTask attributes have a predictable order
        workflow = await get_workflow(client, project.id, wf_id)
        old_worfklowtask_orders = [
            wft["order"] for wft in workflow["task_list"]
        ]
        old_worfklowtask_ids = [wft["id"] for wft in workflow["task_list"]]
        old_task_ids = [wft["task"]["id"] for wft in workflow["task_list"]]
        assert old_worfklowtask_orders == list(range(num_tasks))
        assert old_worfklowtask_ids == list(range(1, num_tasks + 1))
        assert old_task_ids == list(range(1, num_tasks + 1))

        # Call PATCH endpoint to reorder the task_list (and simultaneously
        # update the name attribute)
        NEW_WF_NAME = "new-wf-name"
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}",
            json=dict(
                name=NEW_WF_NAME,
                reordered_workflowtask_ids=reordered_workflowtask_ids,
            ),
        )
        new_workflow = res.json()
        debug(new_workflow)
        assert res.status_code == 200
        assert new_workflow["name"] == NEW_WF_NAME

        # Extract new attribute lists
        new_task_list = new_workflow["task_list"]
        new_workflowtask_orders = [wft["order"] for wft in new_task_list]
        new_workflowtask_ids = [wft["id"] for wft in new_task_list]
        new_task_ids = [wft["task"]["id"] for wft in new_task_list]

        # Assert that new attributes list corresponds to expectations
        assert new_workflowtask_orders == list(range(num_tasks))
        assert new_workflowtask_ids == reordered_workflowtask_ids
        assert new_task_ids == reordered_workflowtask_ids


async def test_reorder_task_list_fail(
    client,
    db,
    MockCurrentUser,
    project_factory,
    task_factory,
):
    """
    GIVEN a workflow with a task_list
    WHEN we call its PATCH endpoint with wrong payload
    THEN the correct errors are raised
    """
    num_tasks = 3

    async with MockCurrentUser(persist=True) as user:
        # Create project, workflow, tasks, workflowtasks
        project = await project_factory(user)
        payload = {"name": "WF"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=payload
        )
        wf_id = res.json()["id"]
        for i in range(num_tasks):
            t = await add_task(client, i)
            res = await client.post(
                (
                    f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                    f"?task_id{t['id']}"
                ),
                json={},
            )

        # Invalid calls to PATCH endpoint to reorder the task_list

        # Invalid payload (not a permutation) leads to pydantic validation
        # error
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}",
            json=dict(reordered_workflowtask_ids=[2, 1, 3, 1]),
        )
        debug(res.json())
        assert res.json()["detail"][0]["type"] == "value_error"
        assert "has repetitions" in res.json()["detail"][0]["msg"]
        assert res.status_code == 422

        # Invalid payload (wrong length) leads to custom fractal-server error
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}",
            json=dict(reordered_workflowtask_ids=[1, 2, 3, 4]),
        )
        debug(res.json())
        assert "must be a permutation" in res.json()["detail"]
        assert res.status_code == 422

        # Invalid payload (wrong values) leads to custom fractal-server error
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}",
            json=dict(reordered_workflowtask_ids=[2, 1, 33]),
        )
        debug(res.json())
        assert "must be a permutation" in res.json()["detail"]
        assert res.status_code == 422


async def test_delete_workflow_failure(
    client,
    MockCurrentUser,
    project_factory,
    job_factory,
    tmp_path,
    workflow_factory,
    dataset_factory,
):
    """
    GIVEN a Workflow in a relationship with a Job
    WHEN we try to DELETE that Workflow
    THEN we fail with a 422
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)

        # Create a workflow and a job in relationship with it
        workflow_1 = await workflow_factory(project_id=project.id)
        input_ds = await dataset_factory(project)
        output_ds = await dataset_factory(project)
        job = await job_factory(
            project_id=project.id,
            workflow_id=workflow_1.id,
            input_dataset_id=input_ds.id,
            output_dataset_id=output_ds.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
        )
        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{workflow_1.id}"
        )
        assert res.status_code == 422
        assert f"still linked to job {job.id}" in res.json()["detail"]

        # Successful workflow deletion
        workflow_2 = await workflow_factory(project_id=project.id)
        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{workflow_2.id}"
        )
        assert res.status_code == 204
