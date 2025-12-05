from typing import Literal

from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusType

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


async def test_post_workflow(db, client, MockCurrentUser, project_factory):
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

        project1 = await project_factory(user)
        project2 = await project_factory(user)
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
    project_factory,
    workflow_factory,
    task_factory,
    dataset_factory,
    job_factory,
    db,
    client,
    MockCurrentUser,
    tmp_path,
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
        workflow = dict(name="My Workflow")

        # Create workflow
        res = await client.post(
            f"{PREFIX}/project/{p_id}/workflow/", json=workflow
        )
        wf_id = res.json()["id"]

        # Create a task
        task = await task_factory(user_id=user.id, name="task1")

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
        stm = select(WorkflowTaskV2).where(WorkflowTaskV2.workflow_id == wf_id)
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
        wf_deletable_1 = await workflow_factory(project_id=project.id)
        wf_deletable_2 = await workflow_factory(project_id=project.id)
        wf_not_deletable_1 = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id, name="task2")
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
        dataset = await dataset_factory(project_id=project.id)
        payload = dict(
            project_id=project.id,
            dataset_id=dataset.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
        )
        j1 = await job_factory(
            workflow_id=wf_deletable_1.id,
            status=JobStatusType.DONE,
            **payload,
        )
        j2 = await job_factory(
            workflow_id=wf_deletable_2.id,
            status=JobStatusType.FAILED,
            **payload,
        )
        await job_factory(
            workflow_id=wf_not_deletable_1.id,
            status=JobStatusType.SUBMITTED,
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
    task_factory,
    project_factory,
    workflow_factory,
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
        t1 = await task_factory(user_id=user_A_id, name="1")
        t2 = await task_factory(user_id=user_A_id, name="2")
    async with MockCurrentUser() as user_B:
        t3 = await task_factory(user_id=user_B.id, name="3")
    tg3 = await db.get(TaskGroupV2, t3.taskgroupv2_id)
    tg2 = await db.get(TaskGroupV2, t2.taskgroupv2_id)
    tg3.user_group_id = None
    tg2.active = False
    db.add(tg2)
    db.add(tg3)
    await db.commit()

    async with MockCurrentUser(user_id=user_A_id) as user_A:
        project = await project_factory(user_A)
        p_id = project.id

        # Create workflow
        WORKFLOW_NAME = "My Workflow"
        wf = await workflow_factory(project_id=p_id, name=WORKFLOW_NAME)
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
        assert res.json()["name"] == WORKFLOW_NAME
        assert res.json()["project"] == EXPECTED_PROJECT

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


async def test_patch_workflow(
    client, MockCurrentUser, project_factory, db, task_factory
):
    """
    GIVEN a Workflow
    WHEN the endpoint to PATCH a Workflow is called
    THEN the Workflow is updated
    """
    # Create several kinds of tasks
    async with MockCurrentUser() as user_A:
        user_A_id = user_A.id
        t1 = await task_factory(user_id=user_A_id, name="1")
        t2 = await task_factory(user_id=user_A_id, name="2")
    async with MockCurrentUser() as user_B:
        t3 = await task_factory(user_id=user_B.id, name="3")
    tg3 = await db.get(TaskGroupV2, t3.taskgroupv2_id)
    tg2 = await db.get(TaskGroupV2, t2.taskgroupv2_id)
    tg3.user_group_id = None
    tg2.active = False
    db.add(tg2)
    db.add(tg3)
    await db.commit()

    async with MockCurrentUser() as user:
        project = await project_factory(user)

        # POST a Workflow with name `WF`
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=dict(name="WF")
        )
        assert res.json()["name"] == "WF"
        wf_id = res.json()["id"]

        for task in [t1, t2, t3]:
            await _workflow_insert_task(
                workflow_id=wf_id, task_id=task.id, db=db
            )

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
        assert res.status_code == 200
        # Assert warnings
        assert res.json()["task_list"][0]["warning"] is None
        assert res.json()["task_list"][1]["warning"] == "Task is not active."
        assert (
            res.json()["task_list"][2]["warning"]
            == "Current user has no access to this task."
        )

        new_workflow = await get_workflow(client, project.id, wf_id)
        assert new_workflow["name"] == "new_WF"

        res = await client.get(f"{PREFIX}/project/{project.id}/workflow/")
        assert len(res.json()) == 2


async def test_delete_workflow_with_job(
    client,
    MockCurrentUser,
    project_factory,
    job_factory,
    task_factory,
    workflow_factory,
    dataset_factory,
    tmp_path,
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
        task = await task_factory(user_id=user.id, name="1")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory(project_id=project.id)

        job = await job_factory(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            status=JobStatusType.DONE,
        )

        assert job.workflow_id == workflow.id

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/"
        )
        assert res.status_code == 204

        await db.refresh(job)
        assert job.workflow_id is None


async def test_workflow_type_filters_flow(
    client,
    MockCurrentUser,
    task_factory,
    project_factory,
    workflow_factory,
    db,
):
    async with MockCurrentUser() as user:
        proj = await project_factory(user)
        wf = await workflow_factory(project_id=proj.id)

        # FAILURE due to empty workflow
        res = await client.get(
            f"{PREFIX}/project/{proj.id}/workflow/{wf.id}/type-filters-flow/"
        )
        assert res.status_code == 422
        assert "Workflow has no tasks" in str(res.json())

        # Add a workflow task
        task_converter = await task_factory(user_id=user.id, name="converter")
        task_cellpose = await task_factory(user_id=user.id, name="cellpose")
        task_MIP = await task_factory(
            user_id=user.id,
            name="mip",
            input_types={"is_3D": True},
            output_types={"is_3D": False},
        )
        wftask_converter = await _workflow_insert_task(
            workflow_id=wf.id,
            task_id=task_converter.id,
            db=db,
        )
        wftask_mip = await _workflow_insert_task(
            workflow_id=wf.id,
            task_id=task_MIP.id,
            db=db,
        )
        wftask_cellpose_2d = await _workflow_insert_task(
            workflow_id=wf.id,
            task_id=task_cellpose.id,
            db=db,
        )
        wftask_cellpose_3d = await _workflow_insert_task(
            workflow_id=wf.id,
            task_id=task_cellpose.id,
            db=db,
            type_filters={"is_3D": True},
        )

        # SUCCESS
        res = await client.get(
            f"{PREFIX}/project/{proj.id}/workflow/{wf.id}/type-filters-flow/",
        )
        assert res.status_code == 200
        expected_response = [
            dict(
                workflowtask_id=wftask_converter.id,
                current_type_filters={},
                input_type_filters={},
                output_type_filters={},
            ),
            dict(
                workflowtask_id=wftask_mip.id,
                current_type_filters={},
                input_type_filters={"is_3D": True},
                output_type_filters={"is_3D": False},
            ),
            dict(
                workflowtask_id=wftask_cellpose_2d.id,
                current_type_filters={"is_3D": False},
                input_type_filters={},
                output_type_filters={},
            ),
            dict(
                workflowtask_id=wftask_cellpose_3d.id,
                current_type_filters={"is_3D": False},
                input_type_filters={"is_3D": True},
                output_type_filters={},
            ),
        ]

        debug(res.json())
        assert res.json() == expected_response
