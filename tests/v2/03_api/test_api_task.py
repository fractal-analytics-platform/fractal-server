import pytest
from devtools import debug

from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskUpdateV2

PREFIX = "/api/v2/task"


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


async def test_task_get_list(db, client, task_factory_v2, MockCurrentUser):

    async with MockCurrentUser() as user:
        await task_factory_v2(user_id=user.id, index=1)
        await task_factory_v2(user_id=user.id, index=2)
        t = await task_factory_v2(
            user_id=user.id,
            index=3,
            args_schema_non_parallel=dict(a=1),
            args_schema_parallel=dict(b=2),
        )
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        assert len(data) == 3
        assert data[2]["id"] == t.id
        assert data[2]["args_schema_non_parallel"] == dict(a=1)
        assert data[2]["args_schema_parallel"] == dict(b=2)

        res = await client.get(f"{PREFIX}/?args_schema_non_parallel=false")
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] == dict(b=2)

        res = await client.get(f"{PREFIX}/?args_schema_parallel=false")
        assert res.json()[2]["args_schema_non_parallel"] == dict(a=1)
        assert res.json()[2]["args_schema_parallel"] is None

        res = await client.get(
            f"{PREFIX}/"
            "?args_schema_parallel=false&args_schema_non_parallel=False"
        )
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] is None


async def test_post_task(client, MockCurrentUser):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        TASK_OWNER = user.username or user.settings.slurm_user
        TASK_SOURCE = "some_source"

        # Successful task creations
        task = TaskCreateV2(
            name="task_name",
            # Compound
            source=f"{TASK_SOURCE}-compound",
            command_parallel="task_command_parallel",
            command_non_parallel="task_command_non_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["name"] == task.name
        assert res.json()["type"] == "compound"
        assert res.json()["command_non_parallel"] == task.command_non_parallel
        assert res.json()["command_parallel"] == task.command_parallel
        assert res.json()["source"] == f"{TASK_OWNER}:{task.source}"
        assert res.json()["meta_non_parallel"] == {}
        assert res.json()["meta_parallel"] == {}
        assert res.json()["version"] is None
        assert res.json()["args_schema_non_parallel"] is None
        assert res.json()["args_schema_parallel"] is None
        assert res.json()["args_schema_version"] is None
        assert res.json()["docs_info"] is None
        assert res.json()["docs_link"] is None
        assert res.json()["input_types"] == {}
        assert res.json()["output_types"] == {}

        task = TaskCreateV2(
            name="task_name",
            # Parallel
            source=f"{TASK_SOURCE}-parallel",
            command_parallel="task_command_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["type"] == "parallel"
        task = TaskCreateV2(
            name="task_name",
            # Non Parallel
            source=f"{TASK_SOURCE}-non_parallel",
            command_non_parallel="task_command_non_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["type"] == "non_parallel"

        # Fail for repeated task.source
        task = TaskCreateV2(
            name="new_task_name",
            source=f"{TASK_SOURCE}-compound",
            command_parallel="new_task_command",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 422

        # Fail for wrong payload
        res = await client.post(f"{PREFIX}/")  # request without body
        debug(res.json())
        assert res.status_code == 422

    # Test multiple combinations of (username, slurm_user)
    SLURM_USER = "some_slurm_user"
    USERNAME = "some_username"
    # Case 1: (username, slurm_user) = (None, None)
    user_kwargs = dict(username=None, is_verified=True)
    user_settings_dict = dict(slurm_user=None)
    payload = dict(name="task", command_parallel="cmd")
    payload["source"] = "source_x"
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot add a new task because current user does not have "
            "`username` or `slurm_user` attributes."
        )
    # Case 2: (username, slurm_user) = (not None, not None)
    user_kwargs = dict(username=USERNAME, is_verified=True)
    user_settings_dict = dict(slurm_user=SLURM_USER)
    payload["source"] = "source_y"
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
    # Case 3: (username, slurm_user) = (None, not None)
    user_kwargs = dict(username=None, is_verified=True)
    user_settings_dict = dict(slurm_user=SLURM_USER)
    payload["source"] = "source_z"
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
    # Case 4: (username, slurm_user) = (not None, None)
    user_kwargs = dict(username=USERNAME, is_verified=True)
    user_settings_dict = dict(slurm_user=None)
    payload["source"] = "source_xyz"
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201

    # Fail giving "non parallel" args to "parallel" tasks, and vice versa
    res = await client.post(
        f"{PREFIX}/",
        json=dict(
            name="name",
            source="xxx",
            command_parallel="cmd",
            args_schema_non_parallel={"a": "b"},
        ),
    )
    assert res.status_code == 422
    assert "Cannot set" in res.json()["detail"]
    res = await client.post(
        f"{PREFIX}/",
        json=dict(
            name="name",
            source="xxx",
            command_parallel="cmd",
            meta_non_parallel={"a": "b"},
        ),
    )
    assert res.status_code == 422
    assert "Cannot set" in res.json()["detail"]

    res = await client.post(
        f"{PREFIX}/",
        json=dict(
            name="name",
            source="xxx",
            command_non_parallel="cmd",
            args_schema_parallel={"a": "b"},
        ),
    )
    assert res.status_code == 422
    assert "Cannot set" in res.json()["detail"]
    res = await client.post(
        f"{PREFIX}/",
        json=dict(
            name="name",
            source="xxx",
            command_non_parallel="cmd",
            meta_parallel={"a": "b"},
        ),
    )
    assert res.status_code == 422
    assert "Cannot set" in res.json()["detail"]


async def test_post_task_without_default_group(
    client,
    MockCurrentUser,
    monkeypatch,
):
    monkeypatch.setattr(
        "fractal_server.app.routes.auth._aux_auth.FRACTAL_DEFAULT_GROUP_NAME",
        "MONKEY",
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/", json=dict(name="a", source="b", command_parallel="c")
        )
        assert res.status_code == 404


async def test_patch_task_auth(
    MockCurrentUser,
    client,
    task_factory_v2,
):
    """
    GIVEN a Task `A` with owner Alice and a Task `N` with owner None
    WHEN Alice, Bob and a Superuser try to patch them
    THEN Alice can edit `A`, Bob cannot edit anything
         and the Superuser can edit both A and N.
    """
    USER_1 = "Alice"
    USER_2 = "Bob"

    async with MockCurrentUser(
        user_kwargs={"username": USER_1, "is_verified": True}
    ) as user:
        task_with_no_owner = await task_factory_v2(user_id=user.id)
        task_with_no_owner_id = task_with_no_owner.id
        task = TaskCreateV2(
            name="task_name",
            source="task_source",
            command_parallel="task_command",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201

        task_id = res.json()["id"]

        # Test success: owner == user
        update = TaskUpdateV2(name="new_name_1")
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_1"

    async with MockCurrentUser(
        user_kwargs={"is_verified": True},
        user_settings_dict={"slurm_user": USER_2},
    ):
        update = TaskUpdateV2(name="new_name_2")

        # Test fail: (not user.is_superuser) and (owner != user)
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            f"Current user ({USER_2}) cannot modify TaskV2 {task_id} "
            f"with different owner ({USER_1})."
        )

        # Test fail: (not user.is_superuser) and (owner == None)
        res = await client.patch(
            f"{PREFIX}/{task_with_no_owner_id}/",
            json=update.dict(exclude_unset=True),
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "Only a superuser can modify a TaskV2 with `owner=None`."
        )

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True, "is_verified": True}
    ):
        res = await client.get(f"{PREFIX}/{task_id}/")
        assert res.json()["name"] == "new_name_1"

        # Test success: (owner != user) but (user.is_superuser)
        update = TaskUpdateV2(name="new_name_3")
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_3"

        # Test success: (owner == None) but (user.is_superuser)
        update = TaskUpdateV2(name="new_name_4")
        res = await client.patch(
            f"{PREFIX}/{task_with_no_owner_id}/",
            json=update.dict(exclude_unset=True),
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_4"


async def test_patch_task(
    task_factory_v2,
    MockCurrentUser,
    client,
):

    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True, is_verified=True)
    ) as user:
        task_parallel = await task_factory_v2(
            user_id=user.id, index=1, type="parallel"
        )
        task_non_parallel = await task_factory_v2(
            user_id=user.id, index=2, type="non_parallel"
        )
        task_compound = await task_factory_v2(user_id=user.id, index=3)
        # Test successuful patch of task_compound
        update = TaskUpdateV2(
            name="new_name",
            version="new_version",
            input_types={"input": True, "output": False},
            output_types={"input": False, "output": True},
            command_parallel="new_cmd_parallel",
            command_non_parallel="new_cmd_non_parallel",
        )
        payload = update.dict(exclude_unset=True)
        res = await client.patch(
            f"{PREFIX}/{task_compound.id}/",
            json=payload,
        )
        assert res.status_code == 200
        for k, v in res.json().items():
            if k in payload.keys():
                # assert patched items have changed
                assert v == payload[k]
            else:
                # assert non patched items are still the same
                assert v == task_compound.model_dump()[k]

    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True, is_verified=True)
    ):
        # Fail on updating unsetted commands
        update_non_parallel = TaskUpdateV2(command_non_parallel="xxx")
        res_compound = await client.patch(
            f"{PREFIX}/{task_compound.id}/",
            json=update_non_parallel.dict(exclude_unset=True),
        )
        res_non_parallel = await client.patch(
            f"{PREFIX}/{task_non_parallel.id}/",
            json=update_non_parallel.dict(exclude_unset=True),
        )
        res_parallel = await client.patch(
            f"{PREFIX}/{task_parallel.id}/",
            json=update_non_parallel.dict(exclude_unset=True),
        )
        assert res_compound.status_code == 200
        assert res_non_parallel.status_code == 200
        assert res_parallel.status_code == 422

        update_parallel = TaskUpdateV2(command_parallel="yyy")
        res_compound = await client.patch(
            f"{PREFIX}/{task_compound.id}/",
            json=update_non_parallel.dict(exclude_unset=True),
        )
        res_non_parallel = await client.patch(
            f"{PREFIX}/{task_non_parallel.id}/",
            json=update_parallel.dict(exclude_unset=True),
        )
        res_parallel = await client.patch(
            f"{PREFIX}/{task_parallel.id}/",
            json=update_parallel.dict(exclude_unset=True),
        )
        assert res_compound.status_code == 200
        assert res_non_parallel.status_code == 422
        assert res_parallel.status_code == 200


@pytest.mark.parametrize("username", (None, "myself"))
@pytest.mark.parametrize("slurm_user", (None, "myself_slurm"))
@pytest.mark.parametrize("owner", (None, "another_owner"))
async def test_patch_task_different_users(
    MockCurrentUser,
    client,
    task_factory_v2,
    username,
    slurm_user,
    owner,
):
    """
    Test that the `username` or `slurm_user` attributes of a (super)user do not
    affect their ability to patch a task. They do raise warnings, but the PATCH
    endpoint returns correctly.
    """
    # User kwargs
    user_payload = {}
    if username:
        user_payload["username"] = username
    if slurm_user:
        user_payload["slurm_user"] = slurm_user

    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True, is_verified=True, **user_payload)
    ) as user:
        task = await task_factory_v2(user_id=user.id, name="task", owner=owner)
        assert task.owner == owner

        # Patch task
        NEW_NAME = "new name"
        payload = TaskUpdateV2(name=NEW_NAME).dict(exclude_unset=True)
        res = await client.patch(f"{PREFIX}/{task.id}/", json=payload)
        debug(res.json())
        assert res.status_code == 200
        assert res.json()["name"] == NEW_NAME


async def test_get_task(task_factory_v2, client, MockCurrentUser):
    async with MockCurrentUser() as user:
        task = await task_factory_v2(user_id=user.id, name="name")
        res = await client.get(f"{PREFIX}/{task.id}/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{task.id+999}/")
        assert res.status_code == 404
        assert "not found" in str(res.json()["detail"])


async def test_delete_task(
    db,
    client,
    MockCurrentUser,
    workflow_factory_v2,
    project_factory_v2,
    task_factory_v2,
    workflowtask_factory_v2,
):
    async with MockCurrentUser():
        res = await client.delete(f"{PREFIX}/12345/")
        assert res.status_code == 405
