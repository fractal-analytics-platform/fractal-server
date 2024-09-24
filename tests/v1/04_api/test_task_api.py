import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v1 import Task
from fractal_server.app.schemas.v1.task import TaskCreateV1
from fractal_server.app.schemas.v1.task import TaskUpdateV1

PREFIX = "/api/v1/task"


async def test_non_verified_user(client, MockCurrentUser):
    """
    Test that a non-verified user is not authorized to make POST/PATCH task
    cals.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(f"{PREFIX}/", json={})
        assert res.status_code == 401

        res = await client.patch(f"{PREFIX}/123/", json={})
        assert res.status_code == 401


async def test_task_get_list(db, client, task_factory, MockCurrentUser):
    t0 = await task_factory(name="task0", source="source0")
    t1 = await task_factory(name="task1", source="source1")
    t2 = await task_factory(
        index=2, subtask_list=[t0, t1], args_schema=dict(a=1)
    )

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        debug(data)
        assert len(data) == 3
        assert data[2]["id"] == t2.id
        assert data[2]["args_schema"] == {"a": 1}

        res = await client.get(f"{PREFIX}/?args_schema=false")
        data = res.json()
        debug(data)
        assert data[2]["args_schema"] is None


async def test_post_task(client, MockCurrentUser):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        TASK_OWNER = user.username or user.slurm_user
        TASK_SOURCE = "some_source"

        # Successful task creation
        VERSION = "1.2.3"
        task = TaskCreateV1(
            name="task_name",
            command="task_command",
            source=TASK_SOURCE,
            input_type="task_input_type",
            output_type="task_output_type",
            version=VERSION,
        )
        payload = task.dict(exclude_unset=True)
        res = await client.post(f"{PREFIX}/", json=payload)
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["version"] == VERSION
        assert res.json()["owner"] == TASK_OWNER
        assert res.json()["source"] == f"{TASK_OWNER}:{TASK_SOURCE}"

        # Fail for repeated task.source
        new_task = TaskCreateV1(
            name="new_task_name",
            command="new_task_command",
            source=TASK_SOURCE,  # same source as `task`
            input_type="new_task_input_type",
            output_type="new_task_output_type",
        )
        payload = new_task.dict(exclude_unset=True)
        res = await client.post(f"{PREFIX}/", json=payload)
        debug(res.json())
        assert res.status_code == 422

        # Fail for wrong payload
        res = await client.post(f"{PREFIX}/")  # request without body
        debug(res.json())
        assert res.status_code == 422

    # Test multiple combinations of (username, slurm_user)
    SLURM_USER = "some_slurm_user"
    USERNAME = "some_username"
    # Case 1: (username, slurm_user) = (None, None)
    user_kwargs = dict(username=None, slurm_user=None, is_verified=True)
    payload["source"] = "source_1"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot add a new task because current user does not have "
            "`username` or `slurm_user` attributes."
        )
    # Case 2: (username, slurm_user) = (not None, not None)
    user_kwargs = dict(
        username=USERNAME, slurm_user=SLURM_USER, is_verified=True
    )
    payload["source"] = "source_2"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == USERNAME
    # Case 3: (username, slurm_user) = (None, not None)
    user_kwargs = dict(username=None, slurm_user=SLURM_USER, is_verified=True)
    payload["source"] = "source_3"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == SLURM_USER
    # Case 4: (username, slurm_user) = (not None, None)
    user_kwargs = dict(username=USERNAME, slurm_user=None, is_verified=True)
    payload["source"] = "source_4"
    async with MockCurrentUser(user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == USERNAME


async def test_patch_task_auth(
    MockCurrentUser,
    client,
    task_factory,
):
    """
    GIVEN a Task `A` with owner Alice and a Task `N` with owner None
    WHEN Alice, Bob and a Superuser try to patch them
    THEN Alice can edit `A`, Bob cannot edit anything
         and the Superuser can edit both A and N.
    """
    USER_1 = "Alice"
    USER_2 = "Bob"

    task_with_no_owner = await task_factory()
    task_with_no_owner_id = task_with_no_owner.id

    async with MockCurrentUser(
        user_kwargs={"username": USER_1, "is_verified": True}
    ):
        task = TaskCreateV1(
            name="task_name",
            command="task_command",
            source="task_source",
            input_type="task_input_type",
            output_type="task_output_type",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["owner"] == USER_1

        task_id = res.json()["id"]

        # Test success: owner == user
        update = TaskUpdateV1(name="new_name_1")
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_1"

    async with MockCurrentUser(
        user_kwargs={"slurm_user": USER_2, "is_verified": True}
    ):
        update = TaskUpdateV1(name="new_name_2")

        # Test fail: (not user.is_superuser) and (owner != user)
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            f"Current user ({USER_2}) cannot modify Task {task_id} "
            f"with different owner ({USER_1})."
        )

        # Test fail: (not user.is_superuser) and (owner == None)
        res = await client.patch(
            f"{PREFIX}/{task_with_no_owner_id}/",
            json=update.dict(exclude_unset=True),
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "Only a superuser can modify a Task with `owner=None`."
        )

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True, "is_verified": True}
    ):
        res = await client.get(f"{PREFIX}/{task_id}/")
        assert res.json()["name"] == "new_name_1"

        # Test success: (owner != user) but (user.is_superuser)
        update = TaskUpdateV1(name="new_name_3")
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_3"

        # Test success: (owner == None) but (user.is_superuser)
        update = TaskUpdateV1(name="new_name_4")
        res = await client.patch(
            f"{PREFIX}/{task_with_no_owner_id}/",
            json=update.dict(exclude_unset=True),
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_4"


async def test_patch_task(
    db,
    task_factory,
    MockCurrentUser,
    client,
):
    task = await task_factory(name="task")

    old_source = task.source
    debug(task)
    NEW_NAME = "new name"
    NEW_INPUT_TYPE = "new input_type"
    NEW_OUTPUT_TYPE = "new output_type"
    NEW_COMMAND = "new command"
    NEW_SOURCE = "new source"
    NEW_META = {"key3": "3", "key4": "4"}
    NEW_VERSION = "1.2.3"
    update = TaskUpdateV1(
        name=NEW_NAME,
        input_type=NEW_INPUT_TYPE,
        output_type=NEW_OUTPUT_TYPE,
        command=NEW_COMMAND,
        source=NEW_SOURCE,
        meta=NEW_META,
        version=NEW_VERSION,
    )

    # Test fails with `source`
    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True, is_verified=True)
    ) as user:
        debug(user)
        res = await client.patch(
            f"{PREFIX}/{task.id}/", json=update.dict(exclude_unset=True)
        )
        debug(res, res.json())
        assert res.status_code == 422
        assert (
            res.json()["detail"] == "patch_task endpoint cannot set `source`"
        )

        # Test successuful without `source`
        res = await client.patch(
            f"{PREFIX}/{task.id}/",
            json=update.dict(exclude_unset=True, exclude={"source"}),
        )
        assert res.status_code == 200
        assert res.json()["name"] == NEW_NAME
        assert res.json()["input_type"] == NEW_INPUT_TYPE
        assert res.json()["output_type"] == NEW_OUTPUT_TYPE
        assert res.json()["command"] == NEW_COMMAND
        assert res.json()["meta"] == NEW_META
        assert res.json()["source"] == old_source
        assert res.json()["version"] == NEW_VERSION
        assert res.json()["owner"] is None

        # Test dictionaries update
        OTHER_META = {"key4": [4, 8, 15], "key0": [16, 23, 42]}
        second_update = TaskUpdateV1(meta=OTHER_META, version=None)
        res = await client.patch(
            f"{PREFIX}/{task.id}/",
            json=second_update.dict(exclude_unset=True),
        )
        debug(res, res.json())
        assert res.status_code == 200
        assert res.json()["name"] == NEW_NAME
        assert res.json()["input_type"] == NEW_INPUT_TYPE
        assert res.json()["output_type"] == NEW_OUTPUT_TYPE
        assert res.json()["command"] == NEW_COMMAND
        assert res.json()["version"] is None
        assert len(res.json()["meta"]) == 3


@pytest.mark.parametrize("username", (None, "myself"))
@pytest.mark.parametrize("slurm_user", (None, "myself_slurm"))
@pytest.mark.parametrize("owner", (None, "another_owner"))
async def test_patch_task_different_users(
    db,
    MockCurrentUser,
    client,
    task_factory,
    username,
    slurm_user,
    owner,
):
    """
    Test that the `username` or `slurm_user` attributes of a (super)user do not
    affect their ability to patch a task. They do raise warnings, but the PATCH
    endpoint returns correctly.
    """

    task = await task_factory(name="task", owner=owner)
    debug(task)
    assert task.owner == owner

    # User kwargs
    user_payload = {}
    if username:
        user_payload["username"] = username
    if slurm_user:
        user_payload["slurm_user"] = slurm_user

    # Patch task
    NEW_NAME = "new name"
    payload = TaskUpdateV1(name=NEW_NAME).dict(exclude_unset=True)
    debug(payload)
    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True, is_verified=True, **user_payload)
    ):
        res = await client.patch(f"{PREFIX}/{task.id}/", json=payload)
        debug(res.json())
        assert res.status_code == 200
        assert res.json()["owner"] == owner
        assert res.json()["name"] == NEW_NAME
        if username:
            assert res.json()["owner"] != username
        if slurm_user:
            assert res.json()["owner"] != slurm_user


async def test_get_task(task_factory, client, MockCurrentUser):
    async with MockCurrentUser():
        task = await task_factory(name="name")
        res = await client.get(f"{PREFIX}/{task.id}/")
        debug(res)
        debug(res.json())
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{task.id+999}/")
        assert res.status_code == 404
        assert res.json()["detail"] == "Task not found"


async def test_delete_task(
    db,
    client,
    MockCurrentUser,
    workflow_factory,
    project_factory,
    task_factory,
    workflowtask_factory,
):
    async with MockCurrentUser(user_kwargs={"username": "bob"}) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        taskA = await task_factory(source="A", owner=user.username)
        taskB = await task_factory(source="B", owner=user.username)
        await workflowtask_factory(workflow_id=workflow.id, task_id=taskA.id)

        # test 422
        res = await client.delete(f"{PREFIX}/{taskA.id}/")
        assert res.status_code == 422
        assert "Cannot remove Task" in res.json()["detail"][0]

        # test success
        task_list = (await db.execute(select(Task))).scalars().all()
        assert len(task_list) == 2
        res = await client.delete(f"{PREFIX}/{taskB.id}/")
        assert res.status_code == 204
        task_list = (await db.execute(select(Task))).scalars().all()
        assert len(task_list) == 1


async def test_patch_args_schema(MockCurrentUser, client):
    """
    Test POST/PATCH endpoints with args_schema attributes. NOTE that the
    args_schema attribute is fully replaced by the one in the request body, in
    the PATCH endpoint.
    """

    async with MockCurrentUser(user_kwargs={"is_verified": True}):
        task = TaskCreateV1(
            name="task_name",
            command="task_command",
            source="some_source",
            input_type="task_input_type",
            output_type="task_output_type",
            version="1",
            args_schema=dict(key1=1, key2=2),
            args_schema_version="1.2.3",
        )
        payload = task.dict(exclude_unset=True)
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201

        task_id = res.json()["id"]
        NEW_VERSION = "pydantic_v1"
        payload = dict(
            args_schema=dict(key2=0, key3=3, key4=4),
            args_schema_version=NEW_VERSION,
        )
        res = await client.patch(f"{PREFIX}/{task_id}/", json=payload)
        assert res.status_code == 200
        new_args_schema = res.json()["args_schema"]
        assert new_args_schema == payload["args_schema"]
        assert res.json()["args_schema_version"] == NEW_VERSION
