from typing import Literal

from devtools import debug  # noqa
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import JobStatusType

PREFIX = "api/v2"


async def get_workflow(client, p_id, wf_id):
    res = await client.get(f"{PREFIX}/project/{p_id}/workflow/{wf_id}/")
    assert res.status_code == 200
    return res.json()


async def post_task(
    client,
    label,
    type: Literal["parallel", "non_parallel", "compound"] = "compound",
):
    task = dict(
        name=f"task{label}",
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


async def test_post_worfkflow_task(
    client,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    task_factory,
    local_resource_profile_db,
):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to POST a new WorkflowTask inside
        the Workflow.task_list is called
    THEN the new WorkflowTask is inserted in Workflow.task_list
    """
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        # Create project and workflow
        proj = await project_factory(user)
        wf = await workflow_factory(project_id=proj.id)
        wf_id = wf.id

        # Test that adding an invalid task fails with 404
        res = await client.post(
            f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
            "?task_id=99999",
            json=dict(),
        )
        debug(res.json())
        assert res.status_code == 404

        # Add valid tasks
        for index in range(2):
            task = await post_task(client, label=index)
            res = await client.post(
                f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
                f"?task_id={task['id']}",
                json=dict(),
            )
            workflow = await get_workflow(client, proj.id, wf_id)
            assert len(workflow["task_list"]) == index + 1
            assert workflow["task_list"][-1]["task"] == task

        workflow = await get_workflow(client, proj.id, wf_id)
        assert len(workflow["task_list"]) == 2

        t2 = await post_task(client, 2)
        args_payload = {"a": 0, "b": 1}
        res = await client.post(
            f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
            f"?task_id={t2['id']}",
            json=dict(args_non_parallel=args_payload),
        )
        assert res.status_code == 201

        # Get back workflow
        workflow = await get_workflow(client, proj.id, wf_id)
        task_list = workflow["task_list"]
        assert len(task_list) == 3
        assert task_list[0]["task"]["name"] == "task0"
        assert task_list[1]["task"]["name"] == "task1"
        assert task_list[2]["task"]["name"] == "task2"
        assert task_list[2]["args_non_parallel"] == args_payload

        # Test type filters compatibility
        task = await task_factory(user_id=user.id, input_types={"a": False})
        res = await client.post(
            f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
            f"?task_id={task.id}",
            json=dict(type_filters={"a": True}),
        )
        assert res.status_code == 422
        assert "filters" in res.json()["detail"]
        res = await client.post(
            f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
            f"?task_id={task.id}",
            json=dict(type_filters={"a": False}),
        )
        assert res.status_code == 201


async def test_post_worfkflow_task_failures(
    client,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    task_factory,
    db,
    local_resource_profile_db,
):
    """
    Setup these tasks, for use by user A:
    * task_A_active -> ok
    * task_A_non_active -> 422 (non active)
    * task_B -> 403 (forbidden)
    """
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user_A:
        user_A_id = user_A.id
        task_A_active = await task_factory(
            name="a-active",
            user_id=user_A_id,
        )
        task_A_non_active = await task_factory(
            name="a-non-active",
            user_id=user_A_id,
            task_group_kwargs=dict(active=False),
        )
    async with MockCurrentUser(profile_id=profile.id) as user_B:
        # Create a new UserGroup with user_B
        new_group = UserGroup(name="new_group")
        db.add(new_group)
        await db.commit()
        await db.refresh(new_group)
        link = LinkUserGroup(user_id=user_B.id, group_id=new_group.id)
        db.add(link)
        await db.commit()
        await db.close()

        user_B_id = user_B.id
        task_B = await task_factory(
            name="a",
            user_id=user_B_id,
            task_group_kwargs=dict(user_group_id=new_group.id),
        )

    async with MockCurrentUser(user_id=user_A_id) as user:
        # Create project and workflow
        proj = await project_factory(user)
        wf = await workflow_factory(project_id=proj.id)
        wf_id = wf.id
        endpoint_path = f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"

        # Valid task
        # Non-active task
        res = await client.post(
            f"{endpoint_path}?task_id={task_A_active.id}",
            json=dict(),
        )
        assert res.status_code == 201

        # Missing task
        res = await client.post(
            f"{endpoint_path}?task_id=99999",
            json=dict(),
        )
        assert res.status_code == 404

        # Non-active task
        res = await client.post(
            f"{endpoint_path}?task_id={task_A_non_active.id}",
            json=dict(),
        )
        assert res.status_code == 422

        # No read access
        res = await client.post(
            f"{endpoint_path}?task_id={task_B.id}",
            json=dict(),
        )
        assert res.status_code == 403

        # Test forbidden request-body attributes
        parallel_task = await post_task(client, label=100, type="parallel")
        non_parallel_task = await post_task(
            client, label=101, type="non_parallel"
        )
        for forbidden in ["meta_non_parallel", "args_non_parallel"]:
            res = await client.post(
                f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
                f"?task_id={parallel_task['id']}",
                json={forbidden: {"a": "b"}},
            )
            assert res.status_code == 422
            assert "Cannot set" in res.json()["detail"]
        for forbidden in ["meta_parallel", "args_parallel"]:
            res = await client.post(
                f"{PREFIX}/project/{proj.id}/workflow/{wf_id}/wftask/"
                f"?task_id={non_parallel_task['id']}",
                json={forbidden: {"a": "b"}},
            )
            assert res.status_code == 422
            assert "Cannot set" in res.json()["detail"]


async def test_delete_workflow_task(
    db,
    client,
    MockCurrentUser,
    project_factory,
    local_resource_profile_db,
):
    """
    GIVEN a Workflow with a list of WorkflowTasks
    WHEN the endpoint to DELETE a WorkflowTask in the
        Workflow.task_list is called
    THEN the selected WorkflowTask is properly removed
        from Workflow.task_list
    """
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        project = await project_factory(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/",
            json=dict(name="My Workflow"),
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        workflow = await get_workflow(client, project.id, wf_id)
        t0 = await post_task(client, 0)
        t1 = await post_task(client, 1)
        t2 = await post_task(client, 2)

        wftasks = []
        for t in [t0, t1, t2]:
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
                f"?task_id={t['id']}",
                json=dict(),
            )
            debug(res.json())
            assert res.status_code == 201
            wftasks.append(res.json())

        assert (
            len((await db.execute(select(WorkflowTaskV2))).scalars().all()) == 3
        )
        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 3
        for i, task in enumerate(workflow["task_list"]):
            assert task["order"] == i

        # Fail because of running Job
        running_job = JobV2(
            workflow_id=workflow["id"],
            status=JobStatusType.SUBMITTED,
            user_email="foo@bar.com",
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=1,
        )
        db.add(running_job)
        await db.commit()
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"{wftasks[1]['id']}/"
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot delete a WorkflowTask while a Job is running for this "
            "Workflow."
        )
        await db.delete(running_job)  # clean up
        await db.commit()

        # Remove the WorkflowTask in the middle
        wf_task_id = wftasks[1]["id"]
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"{wf_task_id}/"
        )
        assert res.status_code == 204

        assert (
            len((await db.execute(select(WorkflowTaskV2))).scalars().all()) == 2
        )
        workflow = await get_workflow(client, project.id, wf_id)
        assert len(workflow["task_list"]) == 2
        for i, task in enumerate(workflow["task_list"]):
            assert task["order"] == i


async def test_patch_workflow_task(
    client,
    MockCurrentUser,
    project_factory,
    task_factory,
    local_resource_profile_db,
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called
    THEN the WorkflowTask is updated
    """
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        project = await project_factory(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        t = await post_task(client, 0)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"?task_id={t['id']}",
            json=dict(),
        )
        assert res.status_code == 201

        workflow = await get_workflow(client, project.id, wf_id)

        assert workflow["task_list"][0]["args_parallel"] is None
        assert workflow["task_list"][0]["args_non_parallel"] is None
        assert workflow["task_list"][0]["meta_non_parallel"] is None
        assert workflow["task_list"][0]["meta_parallel"] is None

        payload = dict(
            args_non_parallel={"a": 111, "b": 222},
            args_parallel={"c": 333, "d": 444},
            meta_non_parallel={"non": "parallel"},
            meta_parallel={"executor": "cpu-low"},
            type_filters={"e": True, "f": False, "g": True},
        )
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=payload,
        )

        patched_workflow_task = res.json()
        debug(patched_workflow_task)
        assert (
            patched_workflow_task["args_non_parallel"]
            == payload["args_non_parallel"]
        )
        assert (
            patched_workflow_task["args_parallel"] == payload["args_parallel"]
        )
        assert (
            patched_workflow_task["meta_non_parallel"]
            == payload["meta_non_parallel"]
        )
        assert (
            patched_workflow_task["meta_parallel"] == payload["meta_parallel"]
        )
        assert patched_workflow_task["type_filters"] == payload["type_filters"]
        assert res.status_code == 200

        payload_up = dict(
            args_non_parallel={"a": {"b": 43}, "c": 2},
            args_parallel={"x": "y"},
            meta_non_parallel={"foo": "bar"},
            meta_parallel={"oof": "arb"},
        )
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=payload_up,
        )
        patched_workflow_task_up = res.json()
        assert (
            patched_workflow_task_up["args_non_parallel"]
            == payload_up["args_non_parallel"]
        )
        assert (
            patched_workflow_task_up["args_parallel"]
            == payload_up["args_parallel"]
        )
        assert (
            patched_workflow_task_up["meta_non_parallel"]
            == payload_up["meta_non_parallel"]
        )
        assert (
            patched_workflow_task_up["meta_parallel"]
            == payload_up["meta_parallel"]
        )
        assert res.status_code == 200

        # Remove an argument
        new_args = patched_workflow_task_up["args_non_parallel"]
        new_args.pop("a")
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=dict(args_non_parallel=new_args),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args_non_parallel"])
        assert "a" not in patched_workflow_task["args_non_parallel"]
        assert res.status_code == 200

        # Remove all arguments
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
            f"wftask/{workflow['task_list'][0]['id']}/",
            json=dict(
                args_non_parallel={},
                type_filters=dict(),
            ),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args_non_parallel"])
        assert patched_workflow_task["args_non_parallel"] is None
        assert patched_workflow_task["type_filters"] == dict()
        assert res.status_code == 200

        # Test 422

        parallel_task = await post_task(client, label=100, type="parallel")
        non_parallel_task = await post_task(
            client, label=101, type="non_parallel"
        )

        parallel_wftask = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"?task_id={parallel_task['id']}",
            json=dict(),
        )
        non_parallel_wftask = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"?task_id={non_parallel_task['id']}",
            json=dict(),
        )

        for forbidden in ["args_non_parallel", "meta_non_parallel"]:
            res = await client.patch(
                f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
                f"wftask/{parallel_wftask.json()['id']}/",
                json={forbidden: {"a": "b"}},
            )
            assert res.status_code == 422
            assert "Cannot patch" in res.json()["detail"]

        for forbidden in ["args_parallel", "meta_parallel"]:
            res = await client.patch(
                f"{PREFIX}/project/{project.id}/workflow/{workflow['id']}/"
                f"wftask/{non_parallel_wftask.json()['id']}/",
                json={forbidden: {"a": "b"}},
            )
            assert res.status_code == 422
            assert "Cannot patch" in res.json()["detail"]

        # Test type filters compatibility
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json={"name": "WorkF"}
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]
        task = await task_factory(user_id=user.id, input_types={"a": False})
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        assert res.status_code == 201
        wft_id = res.json()["id"]

        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/{wft_id}/",
            json={"type_filters": {"a": True}},
        )
        assert res.status_code == 422
        assert "filters" in res.json()["detail"]
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/{wft_id}/",
            json={"type_filters": {"b": True}},
        )
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

    # Prepare models to generate a valid JSON Schema
    class _Arguments(BaseModel):
        a: int
        b: str = "one"
        c: str | None = None
        d: list[int] = [1, 2, 3]

    args_schema = _Arguments.model_json_schema()

    async with MockCurrentUser() as user:
        # Create DB objects
        project = await project_factory(user)
        workflow = {"name": "WF"}
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=workflow
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]
        task = await task_factory(
            user_id=user.id,
            name="task with schema",
            command_non_parallel="cmd",
            args_schema_version="X.Y",
            args_schema_non_parallel=args_schema,
        )
        debug(task)

        task_id = task.id
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(),
        )
        wftask = res.json()
        wftask_id = wftask["id"]
        debug(wftask)
        assert res.status_code == 201

        # First update: modify existing args and add a new one
        payload = dict(
            args_non_parallel={"a": 123, "b": "two", "e": "something"}
        )
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/"
            f"wftask/{wftask_id}/",
            json=payload,
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args_non_parallel"])
        assert patched_workflow_task["args_non_parallel"] == dict(
            a=123, b="two", e="something"
        )
        assert res.status_code == 200

        # Second update: remove all values
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"{wftask_id}/",
            json=dict(args_non_parallel={}),
        )
        patched_workflow_task = res.json()
        debug(patched_workflow_task["args_non_parallel"])
        assert patched_workflow_task["args_non_parallel"] is None
        assert res.status_code == 200


async def test_patch_workflow_task_failures(
    client,
    MockCurrentUser,
    project_factory,
    local_resource_profile_db,
):
    """
    GIVEN a WorkflowTask
    WHEN the endpoint to PATCH a WorkflowTask is called with invalid arguments
    THEN the correct status code is returned
    """

    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        # Prepare two workflows, with one task each
        project = await project_factory(user)
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

        t1 = await post_task(client, 1)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf1_id}/wftask/"
            f"?task_id={t1['id']}",
            json=dict(),
        )

        t2 = await post_task(client, 2)

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf2_id}/wftask/"
            f"?task_id={t2['id']}",
            json=dict(),
        )

        workflow1 = await get_workflow(client, project.id, wf1_id)
        workflow2 = await get_workflow(client, project.id, wf2_id)
        workflow_task = workflow2["task_list"][0]

        # Edit a WorkflowTask for a missing Workflow
        WORKFLOW_ID = 999
        WORKFLOW_TASK_ID = 1
        res = await client.patch(
            (
                f"{PREFIX}/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}/"
            ),
            json={"args_parallel": {"a": 123, "d": 321}},
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
            json={"args_parallel": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 404

        # Edit a valid WorkflowTask without specifying the right Workflow
        WORKFLOW_ID = workflow1["id"]
        WORKFLOW_TASK_ID = workflow_task["id"]
        res = await client.patch(
            (
                f"{PREFIX}/project/{project.id}/workflow/{WORKFLOW_ID}/"
                f"wftask/{WORKFLOW_TASK_ID}/"
            ),
            json={"args": {"a": 123, "d": 321}},
        )
        debug(res.content)
        assert res.status_code == 422


async def test_reorder_task_list(
    project_factory,
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    """
    GIVEN a WorkflowV2 with a task_list
    WHEN we call its PATCH endpoint with the order_permutation attribute
    THEN the task_list is reodered correctly
    """
    reorder_cases = [
        [1, 2],
        [2, 1],
        [1, 2, 3],
        [1, 3, 2],
        [4, 3, 5, 1, 2],
    ]

    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        # Create a main project and a pool of available tasks
        project = await project_factory(user)
        tasks = [(await post_task(client, f"task-{ind}")) for ind in range(5)]

        for ind_perm, permutation in enumerate(reorder_cases):
            num_tasks = len(permutation)

            # Create empty workflow
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/",
                json=dict(name=f"WF-{ind_perm}"),
            )
            assert res.status_code == 201
            wf_id = res.json()["id"]

            # Make no-op API call to reorder an empty task list (only at the
            # first iteration)
            if ind_perm == 0:
                res = await client.patch(
                    f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
                    json=dict(reordered_workflowtask_ids=[]),
                )
                assert res.status_code == 200

            # Create `WorkflowTaskV2` objects
            for ind in range(num_tasks):
                res = await client.post(
                    f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
                    f"?task_id={tasks[ind]['id']}",
                    json=dict(),
                )
                assert res.status_code == 201

            # All WorkflowTask attributes have a predictable order
            workflow = await get_workflow(client, project.id, wf_id)
            task_list = workflow["task_list"]
            reordered_workflowtask_ids = [
                task_list[i - 1]["id"] for i in permutation
            ]
            reordered_task_ids = [
                task_list[i - 1]["task"]["id"] for i in permutation
            ]

            # Call PATCH endpoint to reorder the task_list (and simultaneously
            # update the name attribute)
            NEW_WF_NAME = f"new-wf-name-{ind_perm}"
            res = await client.patch(
                f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
                json=dict(
                    name=NEW_WF_NAME,
                    reordered_workflowtask_ids=reordered_workflowtask_ids,
                ),
            )
            new_workflow = res.json()
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
            assert new_task_ids == reordered_task_ids


async def test_reorder_task_list_fail(
    client,
    MockCurrentUser,
    project_factory,
    db,
    local_resource_profile_db,
):
    """
    GIVEN a workflow with a task_list
    WHEN we call its PATCH endpoint with wrong payload
    THEN the correct errors are raised
    """
    num_tasks = 3

    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        # Create project, workflow, tasks, workflowtasks
        project = await project_factory(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/", json=dict(name="WF")
        )
        wf_id = res.json()["id"]
        for i in range(num_tasks):
            t = await post_task(client, i)
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

        # Fail because of running Job
        running_job = JobV2(
            workflow_id=wf_id,
            status=JobStatusType.SUBMITTED,
            user_email="foo@bar.com",
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=1,
        )
        db.add(running_job)
        running_job2 = JobV2(
            workflow_id=wf_id,
            status=JobStatusType.SUBMITTED,
            user_email="foo@bar.com",
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=1,
        )
        # we add a second running job to test the behavior of
        # `limit(1) + scalar_one_or_none()` in _workflow_has_submitted_job
        db.add(running_job2)
        await db.commit()
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[]),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot re-order WorkflowTasks while a Job is running for this "
            "Workflow."
        )
        await db.delete(running_job)  # clean up
        await db.delete(running_job2)
        await db.commit()
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/",
            json=dict(reordered_workflowtask_ids=[]),
        )
        assert res.status_code == 200


async def test_read_workflowtask(
    MockCurrentUser,
    project_factory,
    client,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as user:
        project = await project_factory(user)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/",
            json=dict(name="My Workflow"),
        )
        assert res.status_code == 201
        wf_id = res.json()["id"]

        t = await post_task(client, 99)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/"
            f"?task_id={t['id']}",
            json=dict(),
        )
        assert res.status_code == 201
        wft_id = res.json()["id"]
        res = await client.get(
            f"{PREFIX}/project/{project.id}/workflow/{wf_id}/wftask/{wft_id}/"
        )
        assert res.status_code == 200
        assert res.json()["task"] == t


async def test_replace_task_in_workflowtask(
    project_factory,
    workflow_factory,
    task_factory,
    workflowtask_factory,
    client,
    MockCurrentUser,
    db,
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        assert workflow.task_list == []

        task1 = await task_factory(name="1", user_id=user.id)
        task2 = await task_factory(name="2", user_id=user.id, type="parallel")
        task3 = await task_factory(name="3", user_id=user.id)
        task4 = await task_factory(
            name="4", user_id=user.id, type="non_parallel"
        )

        wft1 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task1.id,
            args_parallel={"wft1": "wft1"},
            args_non_parallel={"wft1": "wft1"},
            type_filters={"a": True},
        )
        wft2 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task2.id,
            args_parallel={"wft2": "wft2"},
        )
        wft3 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task3.id,
            args_parallel={"wft3": "wft3"},
            args_non_parallel={"wft3": "wft3"},
        )
        wft4 = await workflowtask_factory(
            workflow_id=workflow.id,
            task_id=task4.id,
            args_non_parallel={"wft4": "wft4"},
        )

        await db.refresh(workflow)
        assert [wft.id for wft in workflow.task_list] == [
            wft1.id,
            wft2.id,
            wft3.id,
            wft4.id,
        ]

        task5 = await task_factory(
            name="5",
            user_id=user.id,
            type="compound",
            meta_parallel={"a": 1},
            meta_non_parallel={"b": 2},
            args_schema_parallel={"eee": "fff"},
            args_schema_non_parallel={"ggg": "hhh"},
        )

        # replace task in wft3 with task5
        old_wft3 = wft3.model_dump()
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft3.id}&task_id={task5.id}",
            json={},
        )
        assert res.status_code == 201
        await db.refresh(wft3)
        assert wft3.task.model_dump() == task5.model_dump()
        assert wft3.task_id == task5.id
        assert wft3.args_parallel == old_wft3["args_parallel"]
        assert wft3.args_non_parallel == old_wft3["args_non_parallel"]
        assert wft3.meta_parallel == task5.meta_parallel
        assert wft3.meta_non_parallel == task5.meta_non_parallel

        # Get a fresh workflow from the database
        db.expunge(workflow)
        workflow = await db.get(WorkflowV2, workflow.id)

        # Check that workflowtasks have the correct order and id lists
        wft_orders = [_wftask.order for _wftask in workflow.task_list]
        wft_ids = [_wftask.id for _wftask in workflow.task_list]
        debug(wft_ids)
        debug(wft_orders)
        assert wft_orders == list(range(len(wft_orders)))
        assert wft_ids == [
            wft1.id,
            wft2.id,
            wft3.id,
            wft4.id,
        ]
        # Check that no new workflowtask was created
        res = await db.execute(select(func.count(WorkflowTaskV2.id)))
        wft_count = res.scalar()
        assert wft_count == 4

        # Replace a workflowtask with itself, and check that it was updated
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft1.id}&task_id={task1.id}",
            json={},
        )
        assert res.status_code == 201
        assert res.json()["id"] == wft1.id
        # Check that no new workflowtask was created
        res = await db.execute(select(func.count(WorkflowTaskV2.id)))
        wft_count = res.scalar()
        assert wft_count == 4

        # replace with payload
        # case 1
        payload = dict(args_parallel={"1": "1"}, args_non_parallel={"2": "2"})
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft3.id}&task_id={task3.id}",
            json=payload,
        )
        assert res.status_code == 201
        db.expunge_all()
        wft3 = await db.get(WorkflowTaskV2, wft3.id)
        assert wft3.args_parallel == payload["args_parallel"]
        assert wft3.args_non_parallel == payload["args_non_parallel"]
        # case 2
        payload = dict(args_parallel={"3": "3"})
        old_args_non_parallele = wft3.args_non_parallel
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft3.id}&task_id={task5.id}",
            json=payload,
        )
        assert res.status_code == 201
        await db.refresh(wft3)
        assert wft3.args_parallel == payload["args_parallel"]
        assert wft3.args_non_parallel == old_args_non_parallele
        # case 3:
        # Cannot set 'args_non_parallel' when Task is 'parallel', and v.v.
        payload = dict(args_parallel={"1": "1"}, args_non_parallel={"2": "2"})
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft2.id}&task_id={task2.id}",
            json=payload,
        )
        assert res.status_code == 422
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft4.id}&task_id={task4.id}",
            json=payload,
        )
        assert res.status_code == 422

        # replace with different type
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft1.id}&task_id={task2.id}",
        )
        assert res.status_code == 422
        debug(res.json())

        # Test type filters compatibility
        task6 = await task_factory(user_id=user.id, input_types={"a": False})
        task7 = await task_factory(
            user_id=user.id,
            input_types={"a": True},
            name="7",
        )
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft1.id}&task_id={task6.id}",
            json={},
        )
        assert res.status_code == 422
        assert "filters" in res.json()["detail"]
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"replace-task/?workflow_task_id={wft1.id}&task_id={task7.id}",
            json={},
        )
        assert res.status_code == 201
