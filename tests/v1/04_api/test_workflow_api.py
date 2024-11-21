import json
import logging
from datetime import datetime

import pytest
from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models.v1 import Workflow
from fractal_server.app.models.v1 import WorkflowTask
from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.aux import _raise_if_naive_datetime
from fractal_server.app.schemas.v1 import JobStatusTypeV1
from fractal_server.app.schemas.v1 import WorkflowExportV1
from fractal_server.app.schemas.v1 import WorkflowImportV1
from fractal_server.app.schemas.v1 import WorkflowReadV1


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


async def test_post_workflow(db, client, MockCurrentUser, project_factory):
    async with MockCurrentUser() as user:
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
    db,
    client,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    job_factory,
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
        res = await client.delete(f"api/v1/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 204

        # Check that the Workflow was deleted
        res = await client.get(f"api/v1/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 404

        # Check that the WorkflowTask was deleted
        res = await db.execute(stm)
        res = list(res)
        debug(res)
        assert len(res) == 0

        # Assert you cannot delete a Workflow linked to an ongoing Job
        wf_deletable_1 = await workflow_factory(project_id=project.id)
        wf_deletable_2 = await workflow_factory(project_id=project.id)
        wf_not_deletable_1 = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="source")
        await _workflow_insert_task(
            workflow_id=wf_deletable_1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=wf_deletable_2.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=wf_not_deletable_1.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory(project_id=project.id)
        common_args = {
            "project_id": project.id,
            "input_dataset_id": dataset.id,
            "output_dataset_id": dataset.id,
            "working_dir": (tmp_path / "some_working_dir").as_posix(),
        }
        await job_factory(
            workflow_id=wf_deletable_1.id,
            status=JobStatusTypeV1.DONE,
            **common_args,
        )
        await job_factory(
            workflow_id=wf_deletable_2.id,
            status=JobStatusTypeV1.FAILED,
            **common_args,
        )
        await job_factory(
            workflow_id=wf_not_deletable_1.id,
            status=JobStatusTypeV1.SUBMITTED,
            **common_args,
        )
        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{wf_deletable_1.id}/"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{wf_deletable_2.id}/"
        )
        assert res.status_code == 204
        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{wf_not_deletable_1.id}/"
        )
        assert res.status_code == 422


async def test_get_workflow(client, MockCurrentUser, project_factory):
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
        payload = dict(name=WORFKLOW_NAME, task_list=[])
        res = await client.post(
            f"api/v1/project/{p_id}/workflow/",
            json=payload,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]
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
        _raise_if_naive_datetime(
            datetime.fromisoformat(res.json()["timestamp_created"])
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


async def test_post_worfkflow_task(client, MockCurrentUser, project_factory):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to POST a new WorkflowTask inside
        the Workflow.task_list is called
    THEN the new WorkflowTask is inserted in Workflow.task_list
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory(user)
        workflow = {"name": "My Workflow"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/",
            json=workflow,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        # Test that adding an invalid task fails with 404
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                "?task_id=99999"
            ),
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # Add valid tasks
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
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
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
            (
                "api/v1/"
                f"project/{project.id}/workflow/{wf_id}/wftask/{wf_task_id}/"
            )
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
    async with MockCurrentUser() as user:
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

        workflows = res.json()
        assert len(workflows) == 3
        assert len((await db.execute(select(Workflow))).scalars().all()) == 4


async def test_patch_workflow(client, MockCurrentUser, project_factory):
    """
    GIVEN a Workflow
    WHEN the endpoint to PATCH a Workflow is called
    THEN the Workflow is updated
    """
    async with MockCurrentUser() as user:
        project = await project_factory(user)

        # POST a Workflow with name `WF`
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=dict(name="WF")
        )
        wf_id = res.json()["id"]
        assert res.json()["name"] == "WF"
        res = await client.get(f"api/v1/project/{project.id}/workflow/")
        assert len(res.json()) == 1
        assert res.status_code == 200

        # POST a second Workflow, with name `WF2`
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=dict(name="WF2")
        )
        assert res.status_code == 201

        # fail to PATCH "WF" to "WF2"
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}/",
            json=dict(name="WF2"),
        )
        assert res.status_code == 422
        debug(res.json())
        assert "already exists" in res.json()["detail"]

        patch = {"name": "new_WF"}
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}/", json=patch
        )

        new_workflow = await get_workflow(client, project.id, wf_id)
        assert new_workflow["name"] == "new_WF"
        assert res.status_code == 200

        res = await client.get(f"api/v1/project/{project.id}/workflow/")
        assert len(res.json()) == 2


async def test_patch_workflow_task(client, MockCurrentUser, project_factory):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called
    THEN the WorkflowTask is updated
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
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
            f"api/v1/project/{project.id}/workflow/{workflow['id']}/"
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
            f"api/v1/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=dict(args=new_args),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert "a" not in patched_workflow_task["args"]
        assert res.status_code == 200

        # Remove all arguments
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=dict(args={}),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert patched_workflow_task["args"] is None
        assert res.status_code == 200


async def test_patch_workflow_task_with_args_schema(
    client, MockCurrentUser, project_factory, task_factory
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
        project = await project_factory(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]
        task = await task_factory(
            name="task with schema",
            source="source0",
            command="cmd",
            input_type="Any",
            output_type="Any",
            args_schema_version="X.Y",
            args_schema=args_schema,
        )
        debug(task)
        task_id = task.id
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                f"?task_id={task_id}"
            ),
            json={},
        )
        wftask = res.json()
        wftask_id = wftask["id"]
        debug(wftask)
        assert res.status_code == 201

        # First update: modify existing args and add a new one
        payload = dict(args={"a": 123, "b": "two", "e": "something"})
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}/"
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
            (
                "api/v1/"
                f"project/{project.id}/workflow/{wf_id}/wftask/{wftask_id}/"
            ),
            json=dict(args={}),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args"])
        assert (
            patched_workflow_task["args"] == task.default_args_from_args_schema
        )
        assert res.status_code == 200


async def test_patch_workflow_task_failures(
    client, MockCurrentUser, project_factory
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called with invalid arguments
    THEN the correct status code is returned
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

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
                f"api/v1/project/{project.id}/workflow/{WORKFLOW_ID}/"
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
                f"api/v1/project/{project.id}/workflow/{WORKFLOW_ID}/"
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
                f"api/v1/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}/"
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
    debug(collect_packages)
    existing_source = collect_packages[0].source
    debug(existing_source)
    existing_package_source = ":".join(existing_source.split(":")[:-1])
    debug(existing_package_source)
    task_list = workflow_from_file["task_list"]
    for ind, wftask in enumerate(task_list):
        old_task_source = task_list[ind]["task"]["source"]
        new_task_source = old_task_source.replace(
            "PKG_SOURCE", existing_package_source
        )  # noqa
        task_list[ind]["task"]["source"] = new_task_source
    workflow_from_file["task_list"] = task_list[:]

    debug(workflow_from_file)

    # Create project
    async with MockCurrentUser() as user:
        prj = await project_factory(user)

    # Import workflow into project
    payload = WorkflowImportV1(**workflow_from_file).dict(exclude_none=True)
    debug(payload)
    res = await client.post(
        f"/api/v1/project/{prj.id}/workflow/import/", json=payload
    )
    debug(res.json())
    assert res.status_code == 201
    workflow_imported = res.json()
    debug(workflow_imported)

    # Check that output can be cast to WorkflowRead
    WorkflowReadV1(**workflow_imported)

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

    # Check that the exported workflow is an extension of the one in the
    # original JSON file
    wf_old = WorkflowExportV1(**workflow_from_file).dict(exclude_none=True)
    wf_new = WorkflowExportV1(**workflow_exported).dict(exclude_none=True)
    assert len(wf_old["task_list"]) == len(wf_new["task_list"])
    for task_old, task_new in zip(wf_old["task_list"], wf_new["task_list"]):
        assert task_old.keys() <= task_new.keys()
        if "meta" in task_old:  # then "meta" is also in task_new
            assert task_old["meta"].items() <= task_new["meta"].items()
            task_old.pop("meta")
            task_new.pop("meta")
        elif "meta" in task_new:  # but not in task_old
            task_new.pop("meta")
        assert task_old == task_new


async def test_export_workflow_log(
    client,
    MockCurrentUser,
    task_factory,
    project_factory,
    workflow_factory,
    tmp_path,
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

    # Insert WorkflowTasks
    res = await client.post(
        (
            f"api/v1/project/{prj.id}/workflow/{wf.id}/wftask/"
            f"?task_id={task.id}"
        ),
        json={},
    )
    assert res.status_code == 201

    # Export workflow
    caplog.clear()
    caplog.set_level(logging.WARNING)
    res = await client.get(
        f"/api/v1/project/{prj.id}/workflow/{wf.id}/export/"
    )
    assert res.status_code == 200
    debug(caplog.text)
    assert "not meant to be portable" in caplog.text


async def test_import_export_workflow_fail(
    client,
    MockCurrentUser,
    project_factory,
    testdata_path,
    task_factory,
):
    async with MockCurrentUser() as user:
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

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

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
            f"api/v1/project/{project.id}/workflow/{wf_id}/",
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
            f"api/v1/project/{project.id}/workflow/{wf_id}/",
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

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
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
            f"api/v1/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[2, 1, 3, 1]),
        )
        debug(res.json())
        assert res.json()["detail"][0]["type"] == "value_error"
        assert "has repetitions" in res.json()["detail"][0]["msg"]
        assert res.status_code == 422

        # Invalid payload (wrong length) leads to custom fractal-server error
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[1, 2, 3, 4]),
        )
        debug(res.json())
        assert "must be a permutation" in res.json()["detail"]
        assert res.status_code == 422

        # Invalid payload (wrong values) leads to custom fractal-server error
        res = await client.patch(
            f"api/v1/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[2, 1, 33]),
        )
        debug(res.json())
        assert "must be a permutation" in res.json()["detail"]
        assert res.status_code == 422


async def test_delete_workflow_with_job(
    client,
    MockCurrentUser,
    project_factory,
    job_factory,
    task_factory,
    tmp_path,
    workflow_factory,
    dataset_factory,
    db,
):
    """
    GIVEN a Workflow in a relationship with a Job
    WHEN we DELETE that Workflow
    THEN Job.workflow_id is set to None
    """
    async with MockCurrentUser() as user:

        project = await project_factory(user)

        # Create a workflow and a job in relationship with it
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="1", source="1")

        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        input_ds = await dataset_factory(project_id=project.id)
        output_ds = await dataset_factory(project_id=project.id)

        job = await job_factory(
            project_id=project.id,
            workflow_id=workflow.id,
            input_dataset_id=input_ds.id,
            output_dataset_id=output_ds.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            status=JobStatusTypeV1.DONE,
        )

        assert job.workflow_id == workflow.id

        res = await client.delete(
            f"api/v1/project/{project.id}/workflow/{workflow.id}/"
        )
        assert res.status_code == 204

        await db.refresh(job)
        assert job.workflow_id is None


async def test_read_workflowtask(MockCurrentUser, project_factory, client):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory(user)
        workflow = {"name": "My Workflow"}
        res = await client.post(
            f"api/v1/project/{project.id}/workflow/",
            json=workflow,
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        t = await add_task(client, 99)
        res = await client.post(
            (
                f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/"
                f"?task_id={t['id']}"
            ),
            json={},
        )
        assert res.status_code == 201
        wft_id = res.json()["id"]
        res = await client.get(
            f"api/v1/project/{project.id}/workflow/{wf_id}/wftask/{wft_id}/"
        )
        assert res.status_code == 200
        assert res.json()["task"] == t
