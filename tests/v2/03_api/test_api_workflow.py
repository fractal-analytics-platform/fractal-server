from datetime import datetime
from datetime import timezone

import pytest
from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusTypeV2

PREFIX = "api/v2"


async def get_workflow(client, p_id, wf_id):
    res = await client.get(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
    assert res.status_code == 200
    return res.json()


async def add_task(client, index):
    task = dict(
        name=f"task{index}",
        source=f"source{index}",
        command="cmd",
        type="non_parallel",
    )
    res = await client.post(f"{PREFIX}/task/", json=task)
    assert res.status_code == 201
    return res.json()


async def test_post_workflow(db, client, MockCurrentUser, project_factory_v2):

    async with MockCurrentUser() as user:
        project_id = None
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/",
            json={"name": "My Workflow"},
        )
        assert res.status_code == 422  # no project_id
        res = await client.post(
            f"{PREFIX}/project/123/workflow/",
            json={"name": "My Workflow"},
        )
        assert res.status_code == 404  # project does not exist

        project1 = await project_factory_v2(user)
        project2 = await project_factory_v2(user)
        workflow = dict(name="My Workflow")

        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/", json=workflow
        )
        assert res.status_code == 422  # already in use
        res = await client.post(
            f"{PREFIX}/project/{project2.id}/workflow/", json=workflow
        )
        assert res.status_code == 201  # same name, different projects

        for _id in [project1.id, project2.id]:
            stm = select(WorkflowV2).where(WorkflowV2.project_id == _id)
            _workflow = await db.execute(stm)
            db_workflow = _workflow.scalars().one()

            assert db_workflow.name == workflow["name"]
            assert db_workflow.project_id == _id


async def test_delete_workflow(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    job_factory_v2,
    db,
    client,
    MockCurrentUser,
    tmp_path,
    collect_packages,
):
    """
    GIVEN a Workflow with two Tasks
    WHEN the endpoint that deletes a Workflow is called
    THEN the Workflow and its associated WorkflowTasks are removed from the db
    """
    async with MockCurrentUser() as user:

        # Create project
        project = await project_factory_v2(user)
        p_id = project.id
        workflow = dict(name="My Workflow")

        # Create workflow
        res = await client.post(
            f"{PREFIX}/project/{p_id}/workflow/", json=workflow
        )
        wf_id = res.json()["id"]

        # Add a dummy task to workflow
        res = await client.post(
            f"{PREFIX}/project/{p_id}/workflow/{wf_id}/wftask/",
            json=dict(task_id=collect_packages[0].id, is_legacy_task=True),
        )
        debug(res.json())
        assert res.status_code == 201

        # Verify that the WorkflowTask was correctly inserted into the Workflow
        stm = (
            select(WorkflowTaskV2)
            .join(WorkflowV2)
            .where(WorkflowTaskV2.workflow_id == wf_id)
        )
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 1

        # Delete the Workflow
        res = await client.delete(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 204

        # Check that the Workflow was deleted
        res = await client.get(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 404

        # Check that the WorkflowTask was deleted
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 0

        # Assert you cannot delete a Workflow linked to an ongoing Job
        wf_deletable_1 = await workflow_factory_v2(project_id=project.id)
        wf_deletable_2 = await workflow_factory_v2(project_id=project.id)
        wf_not_deletable_1 = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(name="task", source="source")
        await _workflow_insert_task(
            workflow_id=wf_deletable_1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=wf_deletable_2.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=wf_not_deletable_1.id,
            task_id=task.id,
            db=db,
        )
        dataset = await dataset_factory_v2(project_id=project.id)
        payload = dict(
            project_id=project.id,
            dataset_id=dataset.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
        )
        await job_factory_v2(
            workflow_id=wf_deletable_1.id,
            status=JobStatusTypeV2.DONE,
            **payload,
        )
        await job_factory_v2(
            workflow_id=wf_deletable_2.id,
            status=JobStatusTypeV2.FAILED,
            **payload,
        )
        await job_factory_v2(
            workflow_id=wf_not_deletable_1.id,
            status=JobStatusTypeV2.SUBMITTED,
            **payload,
        )
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_deletable_1.id}/"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_deletable_2.id}/"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_not_deletable_1.id}/"
        )
        assert res.status_code == 422


async def test_get_workflow(client, MockCurrentUser, project_factory_v2):
    """
    GIVEN a Workflow in the db
    WHEN the endpoint to GET a Workflow by its id is called
    THEN the Workflow is returned
    """
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        p_id = project.id
        # Create workflow
        WORFKLOW_NAME = "My Workflow"
        res = await client.post(
            f"{PREFIX}/project/{p_id}/workflow/", json=dict(name=WORFKLOW_NAME)
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]
        # Get project (useful to check workflow.project relationship)
        res = await client.get(f"{PREFIX}/project/{p_id}/")
        assert res.status_code == 200
        EXPECTED_PROJECT = res.json()
        # Get workflow, and check relationship
        res = await client.get(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 200
        debug(res.json())
        assert res.json()["name"] == WORFKLOW_NAME
        assert res.json()["project"] == EXPECTED_PROJECT
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )

        # Get list of project workflows
        res = await client.get(f"{PREFIX}/project/{p_id}/workflow/")
        assert res.status_code == 200
        workflows = res.json()
        assert len(workflows) == 1
        assert workflows[0]["project"] == EXPECTED_PROJECT


async def test_get_user_workflows(
    client, MockCurrentUser, project_factory_v2, workflow_factory_v2
):
    """
    Test /api/v2/workflow/
    """

    async with MockCurrentUser(user_kwargs={}) as user:
        debug(user)

        project1 = await project_factory_v2(user, name="p1")
        project2 = await project_factory_v2(user, name="p2")
        await workflow_factory_v2(project_id=project1.id, name="wf1a")
        await workflow_factory_v2(project_id=project1.id, name="wf1b")
        await workflow_factory_v2(project_id=project2.id, name="wf2a")

        res = await client.get(f"{PREFIX}/workflow/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 3
        assert set(wf["name"] for wf in res.json()) == {"wf1a", "wf1b", "wf2a"}


async def test_post_worfkflow_task(
    client, MockCurrentUser, project_factory_v2
):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to POST a new WorkflowTask inside
        the Workflow.task_list is called
    THEN the new WorkflowTask is inserted in Workflow.task_list
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory_v2(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/",
            json=dict(name="My Workflow"),
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        # Test that adding an invalid task fails with 404
        res = await client.post(
            (f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"),
            json=dict(task_id=99999),
        )
        debug(res.json())
        assert res.status_code == 404

        # Add valid tasks
        for index in range(2):
            t = await add_task(client, index)
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
                json=dict(task_id=t["id"]),
            )
            workflow = await get_workflow(client, project.id, wf_id)
            assert len(workflow["task_list"]) == index + 1
            assert workflow["task_list"][-1]["task"] == t

        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 2

        t2 = await add_task(client, 2)
        args_payload = {"a": 0, "b": 1}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
            json=dict(task_id=t2["id"], args=args_payload),
        )
        assert res.status_code == 201

        t0b = await add_task(client, "0b")
        payload = dict(task_id=t0b["id"], order=1)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
            json=payload,
        )
        assert res.status_code == 201

        # Get back workflow
        workflow = await get_workflow(client, project.id, wf_id)
        task_list = workflow["task_list"]
        assert len(task_list) == 4
        for wftask in task_list:
            assert wftask["is_legacy_task"] is False
        assert task_list[0]["task"]["name"] == "task0"
        assert task_list[1]["task"]["name"] == "task0b"
        assert task_list[2]["task"]["name"] == "task1"
        assert task_list[3]["task"]["name"] == "task2"
        assert task_list[3]["args"] == args_payload


async def test_delete_workflow_task(
    db, client, MockCurrentUser, project_factory_v2
):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to DELETE a WorkflowTask in the
        Workflow.task_list is called
    THEN the selected WorkflowTask is properly removed
        from Workflow.task_list
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory_v2(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/",
            json=dict(name="My Workflow"),
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
                f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
                json=dict(task_id=t["id"]),
            )
            assert res.status_code == 201
            wftasks.append(res.json())

        assert (
            len((await db.execute(select(WorkflowTaskV2))).scalars().all())
            == 3
        )
        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 3
        for i, task in enumerate(workflow["task_list"]):
            assert task["order"] == i

        # Remove the WorkflowTask in the middle
        wf_task_id = wftasks[1]["id"]
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"{wf_task_id}/"
        )
        assert res.status_code == 204

        assert (
            len((await db.execute(select(WorkflowTaskV2))).scalars().all())
            == 2
        )
        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 2
        for i, task in enumerate(workflow["task_list"]):
            assert task["order"] == i


async def test_get_project_workflows(
    db, client, MockCurrentUser, project_factory_v2
):
    """
    GIVEN a Project containing three Workflows
    WHEN the endpoint to GET all the Workflows associated
        to that Project is called
    THEN the list of all its Workflows is returned
    """
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        other_project = await project_factory_v2(user)
        workflow1 = {"name": "WF1"}
        workflow2 = {"name": "WF2"}
        workflow3 = {"name": "WF3"}
        workflow4 = {"name": "WF4"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow1
        )
        assert res.status_code == 201
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow2
        )
        assert res.status_code == 201
        res = await client.post(
            f"{PREFIX}/project/{other_project.id}/workflow/", json=workflow3
        )
        assert res.status_code == 201
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow4
        )
        assert res.status_code == 201

        res = await client.get(f"{PREFIX}/project/{project.id}/workflow/")

        workflows = res.json()
        assert len(workflows) == 3
        assert len((await db.execute(select(WorkflowV2))).scalars().all()) == 4


async def test_patch_workflow(client, MockCurrentUser, project_factory_v2):
    """
    GIVEN a Workflow
    WHEN the endpoint to PATCH a Workflow is called
    THEN the Workflow is updated
    """
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

        # POST a Workflow with name `WF`
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=dict(name="WF")
        )
        wf_id = res.json()["id"]
        assert res.json()["name"] == "WF"
        res = await client.get(f"{PREFIX}/project/{project.id}/workflow/")
        assert len(res.json()) == 1
        assert res.status_code == 200

        # POST a second Workflow, with name `WF2`
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=dict(name="WF2")
        )
        assert res.status_code == 201

        # fail to PATCH "WF" to "WF2"
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(name="WF2"),
        )
        assert res.status_code == 422
        debug(res.json())
        assert "already exists" in res.json()["detail"]

        patch = {"name": "new_WF"}
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/", json=patch
        )

        new_workflow = await get_workflow(client, project.id, wf_id)
        assert new_workflow["name"] == "new_WF"
        assert res.status_code == 200

        res = await client.get(f"{PREFIX}/project/{project.id}/workflow/")
        assert len(res.json()) == 2


async def test_patch_workflow_task(
    client, MockCurrentUser, project_factory_v2
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called
    THEN the WorkflowTask is updated
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        project = await project_factory_v2(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        t = await add_task(client, 0)
        payload = {"task_id": t["id"]}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
            json=dict(task_id=t["id"]),
        )
        assert res.status_code == 201

        workflow = await get_workflow(client, project.id, wf_id)

        assert workflow["task_list"][0]["args"] is None

        payload = dict(args={"a": 123, "d": 321}, meta={"executor": "cpu-low"})
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=payload,
        )

        patched_workflow_task = res.json()
        debug(patched_workflow_task)
        assert patched_workflow_task["args"] == payload["args"]
        assert patched_workflow_task["meta"] == payload["meta"]
        assert res.status_code == 200

        payload_up = dict(args={"a": {"c": 43}, "b": 123})
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=payload_up,
        )
        patched_workflow_task_up = res.json()
        assert patched_workflow_task_up["args"] == payload_up["args"]
        assert res.status_code == 200

        # Remove an argument
        new_args = patched_workflow_task_up["args"]
        new_args.pop("a")
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=dict(args=new_args),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert "a" not in patched_workflow_task["args"]
        assert res.status_code == 200

        # Remove all arguments
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=dict(args={}),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert patched_workflow_task["args"] is None
        assert res.status_code == 200


async def test_patch_workflow_task_with_args_schema(
    client, MockCurrentUser, project_factory_v2, task_factory_v2
):
    """
    GIVEN a Task with args_schema and a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called
    THEN
        it works as expected, that is, it merges the new API-call values with
        the task defaults
    """

    from pydantic import BaseModel
    from typing import Optional

    # Prepare models to generate a valid JSON Schema
    class _Arguments(BaseModel):
        a: int
        b: str = "one"
        c: Optional[str] = None
        d: list[int] = [1, 2, 3]

    args_schema = _Arguments.schema()

    async with MockCurrentUser() as user:
        # Create DB objects
        project = await project_factory_v2(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]
        task = await task_factory_v2(
            name="task with schema",
            source="source0",
            command="cmd",
            args_schema_version="X.Y",
            args_schema=args_schema,
        )
        debug(task)
        task_id = task.id
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
            json=dict(task_id=task_id),
        )
        wftask = res.json()
        wftask_id = wftask["id"]
        debug(wftask)
        assert res.status_code == 201

        # First update: modify existing args and add a new one
        payload = dict(args={"a": 123, "b": "two", "e": "something"})
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/"
            f"wftask/{wftask_id}/",
            json=payload,
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert patched_workflow_task["args"] == dict(
            a=123, b="two", d=[1, 2, 3], e="something"
        )
        assert res.status_code == 200

        # Second update: remove all values
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"{wftask_id}/",
            json=dict(args={}),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert (
            patched_workflow_task["args"] == task.default_args_from_args_schema
        )
        assert res.status_code == 200


async def test_patch_workflow_task_failures(
    client, MockCurrentUser, project_factory_v2
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called with invalid arguments
    THEN the correct status code is returned
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        # Prepare two workflows, with one task each
        project = await project_factory_v2(user)
        workflow1 = {"name": "WF1"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow1
        )
        wf1_id = res.json()["id"]
        workflow2 = {"name": "WF2"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow2
        )
        wf2_id = res.json()["id"]

        t1 = await add_task(client, 1)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf1_id}/wftask/",
            json=dict(task_id=t1["id"]),
        )

        t2 = await add_task(client, 2)

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf2_id}/wftask/",
            json=dict(task_id=t2["id"]),
        )

        workflow1 = await get_workflow(client, project.id, wf1_id)
        workflow2 = await get_workflow(client, project.id, wf2_id)
        workflow_task_1 = workflow1["task_list"][0]
        workflow_task_2 = workflow2["task_list"][0]

        # Modify parallelization_level
        payload = dict(meta={"parallelization_level": "XXX"})
        res = await client.patch(
            (
                f"{PREFIX}/project/{project.id}/workflow/{workflow1['id']}/"
                f"wftask/{workflow_task_1['id']}/"
            ),
            json=payload,
        )
        assert res.status_code == 422

        # Edit a WorkflowTask for a missing Workflow
        WORKFLOW_ID = 999
        WORKFLOW_TASK_ID = 1
        res = await client.patch(
            (
                f"{PREFIX}/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}/"
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
                f"{PREFIX}/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}/"
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
                f"{PREFIX}/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}/"
            ),
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 422


reorder_cases = [
    [1, 2],
    [2, 1],
    [1, 2, 3],
    [1, 3, 2],
    [2, 1, 3],
    [2, 3, 1],
    [3, 2, 1],
    [3, 1, 2],
    [4, 3, 5, 6, 1, 2],
]


@pytest.mark.parametrize("reordered_workflowtask_ids", reorder_cases)
async def test_reorder_task_list(
    reordered_workflowtask_ids,
    project_factory_v2,
    client,
    MockCurrentUser,
):
    """
    GIVEN a WorkflowV2 with a task_list
    WHEN we call its PATCH endpoint with the order_permutation attribute
    THEN the task_list is reodered correctly
    """

    num_tasks = len(reordered_workflowtask_ids)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        # Create project and empty workflow
        project = await project_factory_v2(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=dict(name="WF")
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        # Make no-op API call to reorder an empty task list
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[]),
        )
        assert res.status_code == 200

        # Create tasks and insert WorkflowTasksV2
        for i in range(num_tasks):
            t = await add_task(client, i)
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
                json=dict(task_id=t["id"]),
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
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
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
    MockCurrentUser,
    project_factory_v2,
):
    """
    GIVEN a workflow with a task_list
    WHEN we call its PATCH endpoint with wrong payload
    THEN the correct errors are raised
    """
    num_tasks = 3

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        # Create project, workflow, tasks, workflowtasks
        project = await project_factory_v2(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=dict(name="WF")
        )
        wf_id = res.json()["id"]
        for i in range(num_tasks):
            t = await add_task(client, i)
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
                json=dict(task_id=t["id"]),
            )

        # Invalid calls to PATCH endpoint to reorder the task_list

        # Invalid payload (not a permutation) leads to pydantic validation
        # error
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[2, 1, 3, 1]),
        )
        debug(res.json())
        assert res.json()["detail"][0]["type"] == "value_error"
        assert "has repetitions" in res.json()["detail"][0]["msg"]
        assert res.status_code == 422

        # Invalid payload (wrong length) leads to custom fractal-server error
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[1, 2, 3, 4]),
        )
        debug(res.json())
        assert "must be a permutation" in res.json()["detail"]
        assert res.status_code == 422

        # Invalid payload (wrong values) leads to custom fractal-server error
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[2, 1, 33]),
        )
        debug(res.json())
        assert "must be a permutation" in res.json()["detail"]
        assert res.status_code == 422


async def test_delete_workflow_with_job(
    client,
    MockCurrentUser,
    project_factory_v2,
    job_factory_v2,
    task_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    tmp_path,
    db,
):
    """
    GIVEN a Workflow in a relationship with a Job
    WHEN we DELETE that Workflow
    THEN Job.workflow_id is set to None
    """
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)

        # Create a workflow and a job in relationship with it
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(name="1", source="1")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        job = await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            status=JobStatusTypeV2.DONE,
        )

        assert job.workflow_id == workflow.id

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/"
        )
        assert res.status_code == 204

        await db.refresh(job)
        assert job.workflow_id is None


async def test_read_workflowtask(MockCurrentUser, project_factory_v2, client):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory_v2(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/",
            json=dict(name="My Workflow"),
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        t = await add_task(client, 99)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/",
            json=dict(task_id=t["id"]),
        )
        assert res.status_code == 201
        wft_id = res.json()["id"]
        res = await client.get(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/{wft_id}/"
        )
        assert res.status_code == 200
        assert res.json()["task"] == t


# FIXME
# Missing V2 for:
# - test_import_export_workflow
# - test_export_workflow_log
# - test_import_export_workflow_fail
