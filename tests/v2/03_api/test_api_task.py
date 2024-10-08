import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
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


async def test_task_get_list(
    db, client, task_factory_v2, MockCurrentUser, user_group_factory
):

    async with MockCurrentUser() as user:
        new_group = await user_group_factory(
            group_name="new_group", user_id=user.id
        )

        await task_factory_v2(
            user_id=user.id,
            user_group_id=new_group.id,
            index=1,
            category="Conversion",
            modality="HCS",
            authors="AbCd EFgh,Foo Bar-whatever...",
        )

        await task_factory_v2(
            user_id=user.id,
            index=2,
            category="Conversion",
            modality="EM",
            authors="ABCD EGH",
        )
        t = await task_factory_v2(
            user_id=user.id,
            index=3,
            args_schema_non_parallel=dict(a=1),
            args_schema_parallel=dict(b=2),
            modality="EM",
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

        # Queries
        res = await client.get(f"{PREFIX}/?category=CONVERSION")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/?modality=em")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/?category=conversion&modality=em")
        assert len(res.json()) == 1
        res = await client.get(PREFIX + r"/?author=cd\ e")  # raw for precommit
        assert len(res.json()) == 2

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2


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
            category="Conversion",
            modality="lightsheet",
            authors="Foo Bar + Fractal Team",
            tags=["compound", "test", "post", "api"],
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
        assert res.json()["category"] == "Conversion"
        assert res.json()["modality"] == "lightsheet"
        assert res.json()["authors"] == "Foo Bar + Fractal Team"
        assert res.json()["tags"] == ["compound", "test", "post", "api"]

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


async def test_post_task_user_group_id(
    client,
    default_user_group,
    MockCurrentUser,
    monkeypatch,
    db,
):

    # Create a usergroup
    team1_group = UserGroup(name="team1")
    db.add(team1_group)
    await db.commit()
    await db.refresh(team1_group)

    args = dict(name="a", command_non_parallel="cmd")

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):

        # No query parameter
        res = await client.post(f"{PREFIX}/", json=dict(source="1", **args))
        assert res.status_code == 201
        taskgroup = await db.get(TaskGroupV2, res.json()["taskgroupv2_id"])
        assert taskgroup.user_group_id == default_user_group.id

        # Private task
        res = await client.post(
            f"{PREFIX}/?private=true", json=dict(source="2", **args)
        )
        assert res.status_code == 201
        taskgroup = await db.get(TaskGroupV2, res.json()["taskgroupv2_id"])
        assert taskgroup.user_group_id is None

        # Specific usergroup id / OK
        res = await client.post(
            f"{PREFIX}/?user_group_id={default_user_group.id}",
            json=dict(source="3", **args),
        )
        assert res.status_code == 201
        taskgroup = await db.get(TaskGroupV2, res.json()["taskgroupv2_id"])
        assert taskgroup.user_group_id == default_user_group.id

        # Specific usergroup id / not belonging
        res = await client.post(
            f"{PREFIX}/?user_group_id={team1_group.id}",
            json=dict(source="4", **args),
        )
        assert res.status_code == 403
        debug(res.json())

        # Conflicting query parameters
        res = await client.post(
            f"{PREFIX}/?private=true&user_group_id={default_user_group.id}",
            json=dict(source="5", **args),
        )
        assert res.status_code == 422
        debug(res.json())

        # Default group does not exist
        monkeypatch.setattr(
            (
                "fractal_server.app.routes.auth._aux_auth."
                "FRACTAL_DEFAULT_GROUP_NAME"
            ),
            "MONKEY",
        )
        res = await client.post(f"{PREFIX}/", json=dict(source="4", **args))
        assert res.status_code == 404


async def test_patch_task_auth(
    MockCurrentUser,
    client,
):

    # POST-task as user_A
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user_A:
        user_A_id = user_A.id
        payload_obj = TaskCreateV2(name="a", source="b", command_parallel="c")
        res = await client.post(
            f"{PREFIX}/", json=payload_obj.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        task_id = res.json()["id"]

    # PATCH-task success as user_A -> success (task belongs to user)
    async with MockCurrentUser(user_kwargs=dict(id=user_A_id)) as user_A:
        payload_obj = TaskUpdateV2(name="new_name_1")
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=payload_obj.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_1"

    # PATCH-task failure as a different user -> failure (task belongs to user)
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # PATCH-task failure (task does not belong to user)
        payload_obj = TaskUpdateV2(name="new_name_2")
        res = await client.patch(
            f"{PREFIX}/{task_id}/", json=payload_obj.dict(exclude_unset=True)
        )
        assert res.status_code == 403
        assert "Current user has no full access" in str(res.json()["detail"])


async def test_patch_task(
    task_factory_v2,
    MockCurrentUser,
    client,
):

    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True, is_verified=True)
    ) as user_A:
        user_A_id = user_A.id
        task_parallel = await task_factory_v2(
            user_id=user_A_id, index=1, type="parallel"
        )
        task_non_parallel = await task_factory_v2(
            user_id=user_A_id, index=2, type="non_parallel"
        )
        task_compound = await task_factory_v2(user_id=user_A_id, index=3)
        # Test successuful patch of task_compound
        update = TaskUpdateV2(
            name="new_name",
            version="new_version",
            input_types={"input": True, "output": False},
            output_types={"input": False, "output": True},
            command_parallel="new_cmd_parallel",
            command_non_parallel="new_cmd_non_parallel",
            category="new category",
            modality="new modality",
            authors="New Author 1,New Author 1",
            tags=["new", "tags"],
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

    async with MockCurrentUser(user_kwargs=dict(id=user_A_id)):
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
    client,
    MockCurrentUser,
):
    async with MockCurrentUser():
        res = await client.delete(f"{PREFIX}/12345/")
        assert res.status_code == 405
