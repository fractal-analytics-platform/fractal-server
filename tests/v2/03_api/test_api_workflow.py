import json
import logging
from datetime import datetime
from datetime import timezone
from typing import Literal

from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import WorkflowExportV2
from fractal_server.app.schemas.v2 import WorkflowImportV2
from fractal_server.app.schemas.v2 import WorkflowReadV2

PREFIX = "api/v2"


async def get_workflow(client, p_id, wf_id):
    res = await client.get(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
    assert res.status_code == 200
    return res.json()


async def add_task(
    client,
    index,
    type: Literal["parallel", "non_parallel", "compound"] = "compound",
):
    task = dict(
        name=f"task{index}",
        source=f"source{index}",
        command_non_parallel="cmd",
        command_parallel="cmd",
    )
    if type == "parallel":
        del task["command_non_parallel"]
    elif type == "non_parallel":
        del task["command_parallel"]
    res = await client.post(f"{PREFIX}/task/", json=task)
    debug(res.json())
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

        # Create a task
        task = await task_factory_v2(
            user_id=user.id, name="task", source="dummy"
        )

        # Add a dummy task to workflow
        res = await client.post(
            f"{PREFIX}/project/{p_id}/workflow/{wf_id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        debug(res.json())
        debug(user)
        assert res.status_code == 201

        # Verify that the WorkflowTask was correctly inserted into the Workflow
        stm = (
            select(WorkflowTaskV2)
            .join(WorkflowV2)
            .where(WorkflowTaskV2.workflow_id == wf_id)
        )
        res = await db.execute(stm)
        res = list(res)
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
        assert len(res) == 0

        # Assert you cannot delete a Workflow linked to an ongoing Job
        wf_deletable_1 = await workflow_factory_v2(project_id=project.id)
        wf_deletable_2 = await workflow_factory_v2(project_id=project.id)
        wf_not_deletable_1 = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(
            user_id=user.id, name="task", source="source"
        )
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
        j1 = await job_factory_v2(
            workflow_id=wf_deletable_1.id,
            status=JobStatusTypeV2.DONE,
            **payload,
        )
        j2 = await job_factory_v2(
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
        await db.refresh(j1)
        assert j1.workflow_id is None

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_deletable_2.id}/"
        )
        assert res.status_code == 204
        await db.refresh(j2)
        assert j2.workflow_id is None

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_not_deletable_1.id}/"
        )
        assert res.status_code == 422


async def test_get_workflow(
    client,
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    db,
):
    """
    GIVEN a Workflow in the db
    WHEN the endpoint to GET a Workflow by its id is called
    THEN the Workflow is returned
    """
    # Create several kinds of tasks
    async with MockCurrentUser() as user_A:
        user_A_id = user_A.id
        t1 = await task_factory_v2(user_id=user_A_id, source="1")
        t2 = await task_factory_v2(user_id=user_A_id, source="2")
    async with MockCurrentUser() as user_B:
        t3 = await task_factory_v2(user_id=user_B.id, source="3")
    tg3 = await db.get(TaskGroupV2, t3.taskgroupv2_id)
    tg2 = await db.get(TaskGroupV2, t2.taskgroupv2_id)
    tg3.user_group_id = None
    tg2.active = False
    db.add(tg2)
    db.add(tg3)
    await db.commit()

    async with MockCurrentUser(user_kwargs=dict(id=user_A_id)) as user_A:

        project = await project_factory_v2(user_A)
        p_id = project.id

        # Create workflow
        WORFKLOW_NAME = "My Workflow"
        wf = await workflow_factory_v2(project_id=p_id, name=WORFKLOW_NAME)
        wf_id = wf.id

        for task in [t1, t2, t3]:
            await _workflow_insert_task(
                workflow_id=wf_id, task_id=task.id, db=db
            )

        # Get project (useful to check workflow.project relationship)
        res = await client.get(f"{PREFIX}/project/{p_id}/")
        assert res.status_code == 200
        EXPECTED_PROJECT = res.json()

        # Get workflow, and check relationship
        res = await client.get(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
        assert res.status_code == 200
        assert res.json()["name"] == WORFKLOW_NAME
        assert res.json()["project"] == EXPECTED_PROJECT
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )

        # Assert warnings
        assert res.json()["task_list"][0]["warning"] is None
        assert res.json()["task_list"][1]["warning"] == "Task is not active."
        assert (
            res.json()["task_list"][2]["warning"]
            == "Current user has no access to this task."
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
        task = await task_factory_v2(user_id=user.id, name="1", source="1")
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


async def test_import_export_workflow(
    client,
    MockCurrentUser,
    project_factory_v2,
    task_factory,
    task_factory_v2,
    testdata_path,
):

    # Load workflow to be imported into DB
    with (testdata_path / "import_export/workflow-v2.json").open("r") as f:
        workflow_from_file = json.load(f)

    async with MockCurrentUser() as user:
        # Create project
        prj = await project_factory_v2(user)
        # Add dummy tasks to DB
        await task_factory_v2(
            user_id=user.id, name="task", source="PKG_SOURCE:dummy2"
        )
        await task_factory(name="task", source="PKG_SOURCE:dummy1")

    # Import workflow into project
    payload = WorkflowImportV2(**workflow_from_file).dict(exclude_none=True)

    res = await client.post(
        f"{PREFIX}/project/{prj.id}/workflow/import/", json=payload
    )
    assert res.status_code == 201
    workflow_imported = res.json()

    # Check that output can be cast to WorkflowRead
    WorkflowReadV2(**workflow_imported)

    # Export workflow
    workflow_id = workflow_imported["id"]
    res = await client.get(
        f"{PREFIX}/project/{prj.id}/workflow/{workflow_id}/export/"
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

    wf_old = WorkflowExportV2(**workflow_from_file).dict()
    wf_new = WorkflowExportV2(**workflow_exported).dict()
    assert len(wf_old["task_list"]) == len(wf_new["task_list"])
    for task_old, task_new in zip(wf_old["task_list"], wf_new["task_list"]):
        assert task_old.keys() <= task_new.keys()
        for meta in ["meta_parallel", "meta_non_parallel"]:
            if task_old.get(meta):
                # then 'meta' is also in task_new
                debug(meta)
                assert task_old[meta].items() <= task_new[meta].items()
                task_old.pop(meta)
                task_new.pop(meta)
            elif task_new.get(meta):  # but not in task_old
                task_new.pop(meta)
        debug(task_old, task_new)
        assert task_old == task_new


async def test_export_workflow_log(
    client,
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    caplog,
):
    """
    WHEN exporting a workflow with custom tasks
    THEN there must be a warning
    """

    # Create project and task
    async with MockCurrentUser() as user:
        TASK_OWNER = "someone"
        task = await task_factory_v2(
            user_id=user.id, owner=TASK_OWNER, source="some-source"
        )
        prj = await project_factory_v2(user)
        wf = await workflow_factory_v2(project_id=prj.id)

    # Insert WorkflowTasks
    res = await client.post(
        (
            f"api/v2/project/{prj.id}/workflow/{wf.id}/wftask/"
            f"?task_id={task.id}"
        ),
        json={},
    )
    assert res.status_code == 201

    # Export workflow
    caplog.clear()
    caplog.set_level(logging.WARNING)
    res = await client.get(
        f"/api/v2/project/{prj.id}/workflow/{wf.id}/export/"
    )
    assert res.status_code == 200
    debug(caplog.text)
    assert "not meant to be portable" in caplog.text


async def test_import_export_workflow_fail(
    client,
    MockCurrentUser,
    project_factory_v2,
    task_factory,
):
    async with MockCurrentUser() as user:
        prj = await project_factory_v2(user)

    await task_factory(name="valid", source="test_source")
    payload = {
        "name": "MyWorkflow",
        "task_list": [
            {"order": 0, "task": {"name": "dummy", "source": "xyz"}}
        ],
    }
    res = await client.post(
        f"/api/v2/project/{prj.id}/workflow/import/", json=payload
    )
    assert res.status_code == 422
    assert "Found 0 tasks with source" in res.json()["detail"]
