from pathlib import Path

import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
from fractal_server.app.schemas.v2 import TaskCreate
from fractal_server.app.schemas.v2 import TaskUpdate

PREFIX = "/api/v2/task"


async def test_non_verified_user(client, MockCurrentUser):
    """
    Test that a non-verified user is not authorized to make POST/PATCH task
    cals.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)) as user:
        debug(user)
        res = await client.post(f"{PREFIX}/", json={})
        debug(res.json())
        return
        assert res.status_code == 401

        res = await client.patch(f"{PREFIX}/123/", json={})
        assert res.status_code == 401


async def test_fail_wheel_file_and_version(client, testdata_path):
    local_archive_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()

    with open(local_archive_path, "rb") as f:
        files = {
            "file": (
                Path(local_archive_path).name,
                f.read(),
                "application/zip",
            )
        }
    res = await client.post(
        f"{PREFIX}/collect/pip/",
        data=dict(
            package_version="1.2.3",
            python_version="3.12",
        ),
        files=files,
    )
    assert res.status_code == 422
    error_msg = "Cannot set `package_version` when `file` is provided"
    assert error_msg in str(res.json()["detail"])


async def test_task_get_list(
    db, client, task_factory_v2, MockCurrentUser, user_group_factory
):
    async with MockCurrentUser() as user:
        new_group = await user_group_factory(
            group_name="new_group", user_id=user.id
        )

        await task_factory_v2(
            user_id=user.id,
            task_group_kwargs=dict(user_group_id=new_group.id),
            index=1,
            category="Conversion",
            modality="HCS",
            authors="Name1 Surname1,Name2 Surname2...",
        )

        await task_factory_v2(
            user_id=user.id,
            index=2,
            category="Conversion",
            modality="EM",
            authors="NAME1 SURNAME3",
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

        res = await client.get(f"{PREFIX}/?args_schema=false")
        assert res.json()[2]["args_schema_non_parallel"] is None
        assert res.json()[2]["args_schema_parallel"] is None

        # Queries
        res = await client.get(f"{PREFIX}/?category=CONVERSION")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/?modality=em")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/?category=conversion&modality=em")
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/?author=name1%20sur")
        assert len(res.json()) == 2

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_post_task(
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)):
        # Successful task creations
        task = TaskCreate(
            name="task_name",
            # Compound
            command_parallel="task_command_parallel",
            command_non_parallel="task_command_non_parallel",
            category="Conversion",
            modality="lightsheet",
            authors="Foo Bar + Fractal Team",
            tags=["compound", "test", "post", "api"],
        )
        res = await client.post(
            f"{PREFIX}/", json=task.model_dump(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["name"] == task.name
        assert res.json()["type"] == "compound"
        assert res.json()["command_non_parallel"] == task.command_non_parallel
        assert res.json()["command_parallel"] == task.command_parallel
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

        task = TaskCreate(
            name="task_name",
            command_parallel="task_command_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.model_dump(exclude_unset=True)
        )
        # TaskGroupV2 with same (pkg_name, version, user_id)
        assert res.status_code == 422

        task = TaskCreate(
            name="task_name2",
            # Parallel
            command_parallel="task_command_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.model_dump(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["type"] == "parallel"
        task = TaskCreate(
            name="task_name3",
            # Non Parallel
            command_non_parallel="task_command_non_parallel",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.model_dump(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["type"] == "non_parallel"

        # Fail for wrong payload
        res = await client.post(f"{PREFIX}/")  # request without body
        debug(res.json())
        assert res.status_code == 422

    # Fail giving "non parallel" args to "parallel" tasks, and vice versa
    res = await client.post(
        f"{PREFIX}/",
        json=dict(
            name="name",
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
    override_settings_factory,
    db,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    # Create a usergroup
    team1_group = UserGroup(name="team1")
    db.add(team1_group)
    await db.commit()
    await db.refresh(team1_group)

    args = dict(command_non_parallel="cmd", type="non_parallel")

    async with MockCurrentUser(
        user_kwargs=dict(profile_id=profile.id)
    ):  # No query parameter
        res = await client.post(f"{PREFIX}/", json=dict(name="a", **args))
        assert res.status_code == 201
        taskgroup = await db.get(TaskGroupV2, res.json()["taskgroupv2_id"])
        assert taskgroup.user_group_id == default_user_group.id

        # Private task
        res = await client.post(
            f"{PREFIX}/?private=true", json=dict(name="b", **args)
        )
        assert res.status_code == 201
        taskgroup = await db.get(TaskGroupV2, res.json()["taskgroupv2_id"])
        assert taskgroup.user_group_id is None

        # Specific usergroup id / OK
        res = await client.post(
            f"{PREFIX}/?user_group_id={default_user_group.id}",
            json=dict(name="c", **args),
        )
        assert res.status_code == 201
        taskgroup = await db.get(TaskGroupV2, res.json()["taskgroupv2_id"])
        assert taskgroup.user_group_id == default_user_group.id

        # Specific usergroup id / not belonging
        res = await client.post(
            f"{PREFIX}/?user_group_id={team1_group.id}",
            json=dict(name="d", **args),
        )
        assert res.status_code == 403
        debug(res.json())

        # Conflicting query parameters
        res = await client.post(
            f"{PREFIX}/?private=true&user_group_id={default_user_group.id}",
            json=dict(name="e", **args),
        )
        assert res.status_code == 422
        debug(res.json())

        # Default group does not exist
        with pytest.raises(ValidationError):
            override_settings_factory(FRACTAL_DEFAULT_GROUP_NAME="Monkey")


async def test_patch_task_auth(
    MockCurrentUser,
    client,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    # POST-task as user_A
    async with MockCurrentUser(
        user_kwargs=dict(profile_id=profile.id)
    ) as user_A:
        user_A_id = user_A.id
        payload_obj = TaskCreate(
            name="a", category="my-cat", command_parallel="c"
        )
        res = await client.post(
            f"{PREFIX}/", json=payload_obj.model_dump(exclude_unset=True)
        )
        assert res.status_code == 201
        task_id = res.json()["id"]

    # PATCH-task success as user_A -> success (task belongs to user)
    async with MockCurrentUser(user_kwargs=dict(id=user_A_id)) as user_A:
        payload_obj = TaskUpdate(category="new-cat-1")
        res = await client.patch(
            f"{PREFIX}/{task_id}/",
            json=payload_obj.model_dump(exclude_unset=True),
        )
        assert res.status_code == 200
        assert res.json()["category"] == "new-cat-1"

    # PATCH-task failure as a different user -> failure (task belongs to user)
    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)):
        # PATCH-task failure (task does not belong to user)
        payload_obj = TaskUpdate(category="new-cat-2")
        res = await client.patch(
            f"{PREFIX}/{task_id}/",
            json=payload_obj.model_dump(exclude_unset=True),
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
        update = TaskUpdate(
            input_types={"input": True, "output": False},
            output_types={"input": False, "output": True},
            command_parallel="new_cmd_parallel",
            command_non_parallel="new_cmd_non_parallel",
            category="new category",
            modality="new modality",
            authors="New Author 1,New Author 1",
            tags=["new", "tags"],
        )
        payload = update.model_dump(exclude_unset=True)
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
        update_non_parallel = TaskUpdate(command_non_parallel="xxx")
        res_compound = await client.patch(
            f"{PREFIX}/{task_compound.id}/",
            json=update_non_parallel.model_dump(exclude_unset=True),
        )
        res_non_parallel = await client.patch(
            f"{PREFIX}/{task_non_parallel.id}/",
            json=update_non_parallel.model_dump(exclude_unset=True),
        )
        res_parallel = await client.patch(
            f"{PREFIX}/{task_parallel.id}/",
            json=update_non_parallel.model_dump(exclude_unset=True),
        )
        assert res_compound.status_code == 200
        assert res_non_parallel.status_code == 200
        assert res_parallel.status_code == 422

        update_parallel = TaskUpdate(command_parallel="yyy")
        res_compound = await client.patch(
            f"{PREFIX}/{task_compound.id}/",
            json=update_non_parallel.model_dump(exclude_unset=True),
        )
        res_non_parallel = await client.patch(
            f"{PREFIX}/{task_non_parallel.id}/",
            json=update_parallel.model_dump(exclude_unset=True),
        )
        res_parallel = await client.patch(
            f"{PREFIX}/{task_parallel.id}/",
            json=update_parallel.model_dump(exclude_unset=True),
        )
        assert res_compound.status_code == 200
        assert res_non_parallel.status_code == 422
        assert res_parallel.status_code == 200


async def test_get_task(
    task_factory_v2,
    client,
    MockCurrentUser,
    local_resource_profile_db,
    slurm_sudo_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    resource2, profile2 = slurm_sudo_resource_profile_db

    async with MockCurrentUser(user_kwargs={"profile_id": profile.id}) as user1:
        task1 = await task_factory_v2(user_id=user1.id, name="name1")

    async with MockCurrentUser(
        user_kwargs={"profile_id": profile2.id}
    ) as user2:
        task2 = await task_factory_v2(user_id=user2.id, name="name2")
        res = await client.get(f"{PREFIX}/{task2.id}/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/9999/")
        assert res.status_code == 404
        assert "not found" in str(res.json()["detail"])
        res = await client.get(f"{PREFIX}/{task1.id}/")
        assert res.status_code == 403


async def test_delete_task(
    client,
    MockCurrentUser,
):
    async with MockCurrentUser():
        res = await client.delete(f"{PREFIX}/12345/")
        assert res.status_code == 405
