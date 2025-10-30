from urllib.parse import quote

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2


PREFIX = "/api/v2/task-group"


async def test_get_single_task_group(
    client, MockCurrentUser, task_factory_v2, db
):
    async with MockCurrentUser() as user1:
        # Create a new UserGroup with user1
        new_group = UserGroup(name="new_group")
        db.add(new_group)
        await db.commit()
        await db.refresh(new_group)
        link = LinkUserGroup(user_id=user1.id, group_id=new_group.id)
        db.add(link)
        await db.commit()
        await db.close()

        task = await task_factory_v2(
            user_id=user1.id,
            task_group_kwargs=dict(user_group_id=new_group.id),
        )

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 200
        assert res.json()["user_id"] == user1.id
        assert res.json()["user_group_id"] == new_group.id
        assert len(res.json()["task_list"]) == 1
        assert res.json()["task_list"][0]["id"] == task.id

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id + 1}/")
        assert res.status_code == 404

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id + 1}/")
        assert res.status_code == 404


async def test_get_task_group_list(
    client,
    MockCurrentUser,
    task_factory_v2,
    default_user_group,
    db,
):
    # Create a task-group that belongs to user1. This task group won't be part
    # of the `GET /api/v2/task-group/` response, because it has lower priority
    # than the same task group belonging to user2
    async with MockCurrentUser() as user1:
        task_by_user3 = await task_factory_v2(
            user_id=user1.id,
            task_group_kwargs=dict(
                pkg_name="bbb",
                version="1.0.0",
                user_group_id=default_user_group.id,
            ),
        )

    async with MockCurrentUser() as user2:
        await task_factory_v2(
            user_id=user2.id,
            args_schema_non_parallel={"foo": 0, "bar": 1},
            args_schema_parallel={"xxx": 2, "yyy": 3},
            task_group_kwargs=dict(
                pkg_name="bbb",
                version="1.0.0",
                user_group_id=None,
            ),
        )
        await task_factory_v2(
            user_id=user2.id,
            task_group_kwargs=dict(
                active=False, pkg_name="aaa", version="1.2.3"
            ),
            args_schema_non_parallel={"foo": 4, "bar": 5},
            args_schema_parallel={"xxx": 6, "yyy": 7},
        )
        await task_factory_v2(
            user_id=user2.id,
            task_group_kwargs=dict(
                active=False, pkg_name="bbb", version="xxx"
            ),
        )
        await task_factory_v2(
            user_id=user2.id,
            task_group_kwargs=dict(
                active=False, pkg_name="bbb", version="abc"
            ),
        )
        await task_factory_v2(
            user_id=user2.id,
            task_group_kwargs=dict(
                pkg_name="bbb",
                version=None,
            ),
        )
        await task_factory_v2(
            user_id=user2.id,
            task_group_kwargs=dict(
                active=False, pkg_name="bbb", version="1.0.1"
            ),
        )

        # Verify that the task-group by user1 is accessible
        res = await client.get(f"{PREFIX}/{task_by_user3.taskgroupv2_id}/")
        assert res.status_code == 200
        taskgroup_by_user3 = res.json()

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        result = res.json()
        assert len(result) == 2  # number of unique `pkg_name`s
        assert result[0][0] == "aaa"
        assert result[1][0] == "bbb"
        task_groups_aaa = result[0][1]
        task_groups_bbb = result[1][1]
        # Verify that the task-group by user1 was not included
        task_groups_bbb_ids = [tg["id"] for tg in task_groups_bbb]
        assert taskgroup_by_user3["id"] not in task_groups_bbb_ids
        # Validate the number of elements
        assert len(task_groups_aaa) == 1
        assert len(task_groups_bbb) == 5
        # Verify that versions are sorted
        assert [tg["version"] for tg in task_groups_bbb] == [
            "1.0.1",
            "1.0.0",
            "xxx",
            "abc",
            None,
        ]
        for key in ["args_schema_non_parallel", "args_schema_parallel"]:
            assert task_groups_aaa[0]["task_list"][0][key] is not None

        # Test query parameter `args_schema=false`
        res = await client.get(f"{PREFIX}/?args_schema=false")
        assert res.status_code == 200
        task_groups_aaa = res.json()[0][1]
        for key in ["args_schema_non_parallel", "args_schema_parallel"]:
            assert task_groups_aaa[0]["task_list"][0][key] is None

    async with MockCurrentUser() as user3:
        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2

        # Create a new UserGroup with user2
        new_group = UserGroup(name="new_group")
        db.add(new_group)
        await db.commit()
        await db.refresh(new_group)
        link = LinkUserGroup(user_id=user3.id, group_id=new_group.id)
        db.add(link)
        await db.commit()
        await db.close()

        await task_factory_v2(
            user_id=user3.id,
            task_group_kwargs=dict(user_group_id=new_group.id),
        )

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 3

        res = await client.get(f"{PREFIX}/?only_owner=true")
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/?only_active=true")
        assert res.status_code == 200
        assert len(res.json()) == 2

    async with MockCurrentUser(user_kwargs={"id": user2.id}):
        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_patch_task_group(
    client,
    MockCurrentUser,
    task_factory_v2,
    default_user_group,
    user_group_factory,
):
    async with MockCurrentUser(debug=True) as another_user:
        another_user_id = another_user.id

    async with MockCurrentUser(debug=True) as user1:
        taskA = await task_factory_v2(
            name="asd",
            user_id=user1.id,
            task_group_kwargs=dict(user_group_id=default_user_group.id),
        )
        group2 = await user_group_factory("team2", user1.id, another_user_id)
        taskB = await task_factory_v2(
            name="asd",
            user_id=another_user_id,
            task_group_kwargs=dict(user_group_id=group2.id),
        )

        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["user_group_id"] == default_user_group.id

        # Update: change `user_group_id`
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/",
            json=dict(user_group_id=default_user_group.id),
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["user_group_id"] == default_user_group.id

        # Non existing TaskGroup
        res = await client.patch(f"{PREFIX}/9999999/", json={})
        assert res.status_code == 404

        # Non existing UserGroup
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/", json=dict(user_group_id=42)
        )
        assert res.status_code == 404

        # Re-link the task-group to its current usergroup
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/",
            json=dict(user_group_id=default_user_group.id),
        )
        assert res.status_code == 200

    async with MockCurrentUser(
        debug=True, user_kwargs=dict(id=another_user_id)
    ):
        # Link the task-group to another usergroup and fail due to
        # non-duplication constraint
        res = await client.patch(
            f"{PREFIX}/{taskB.taskgroupv2_id}/",
            json=dict(user_group_id=default_user_group.id),
        )
        assert res.status_code == 422
        assert "already owns a task group" in res.json()["detail"]

    async with MockCurrentUser(debug=True):
        # Unauthorized
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/",
            json={},
        )
        assert res.status_code == 403


async def test_get_single_task_group_activity(client, MockCurrentUser, db):
    async with MockCurrentUser() as user:
        activity = TaskGroupActivityV2(
            user_id=user.id,
            pkg_name="foo",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.COLLECT,
        )
        db.add(activity)
        await db.commit()
        await db.refresh(activity)

        res = await client.get(f"{PREFIX}/activity/{activity.id}/")
        assert res.status_code == 200

        res = await client.get(f"{PREFIX}/activity/{activity.id + 1}/")
        assert res.status_code == 404

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/activity/{activity.id}/")
        assert res.status_code == 403


async def test_get_task_group_activity_list(
    client,
    MockCurrentUser,
    db,
    task_factory_v2,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(profile_id=profile.id)
    ) as user:
        task = await task_factory_v2(
            user_id=user.id,
            task_group_kwargs=dict(resource_id=resource.id),
        )

        activity1 = TaskGroupActivityV2(
            user_id=user.id,
            pkg_name="foo",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.COLLECT,
        )
        activity2 = TaskGroupActivityV2(
            user_id=user.id,
            pkg_name="bar",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.REACTIVATE,
        )
        activity3 = TaskGroupActivityV2(
            user_id=user.id,
            pkg_name="foo",
            version="2",
            status=TaskGroupActivityStatusV2.FAILED,
            action=TaskGroupActivityActionV2.COLLECT,
            taskgroupv2_id=task.taskgroupv2_id,
        )
        activity4 = TaskGroupActivityV2(
            user_id=user.id,
            pkg_name="foo",
            version="1",
            status=TaskGroupActivityStatusV2.OK,
            action=TaskGroupActivityActionV2.COLLECT,
            taskgroupv2_id=task.taskgroupv2_id,
        )
        for activity in [activity1, activity2, activity3, activity4]:
            db.add(activity)
        await db.commit()
        for activity in [activity1, activity2, activity3, activity4]:
            await db.refresh(activity)

        res = await client.get(f"{PREFIX}/activity/")
        assert res.status_code == 200
        assert len(res.json()) == 4

        # taskgroupv2_id
        res = await client.get(
            f"{PREFIX}/activity/?taskgroupv2_id={task.taskgroupv2_id}"
        )
        assert len(res.json()) == 2
        # task_group_activity_id
        res = await client.get(
            f"{PREFIX}/activity/"
            f"?taskgroupv2_id={task.taskgroupv2_id}"
            f"&task_group_activity_id={activity3.id}"
        )
        assert len(res.json()) == 1
        # pkg_name
        res = await client.get(f"{PREFIX}/activity/?pkg_name=foo")
        assert len(res.json()) == 3
        res = await client.get(f"{PREFIX}/activity/?pkg_name=bar")
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/activity/?pkg_name=xxx")
        assert len(res.json()) == 0
        # status
        res = await client.get(f"{PREFIX}/activity/?status=OK")
        assert len(res.json()) == 3
        res = await client.get(f"{PREFIX}/activity/?status=failed")
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/activity/?status=ongoing")
        assert len(res.json()) == 0
        res = await client.get(f"{PREFIX}/activity/?status=xxx")
        assert res.status_code == 422
        # action
        res = await client.get(f"{PREFIX}/activity/?action=collect")
        assert len(res.json()) == 3
        res = await client.get(f"{PREFIX}/activity/?action=reactivate")
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/activity/?action=deactivate")
        assert len(res.json()) == 0
        res = await client.get(f"{PREFIX}/activity/?action=xxx")
        assert res.status_code == 422
        # timestamp_started_min
        res = await client.get(
            f"{PREFIX}/activity/"
            f"?timestamp_started_min={quote(str(activity2.timestamp_started))}"
        )
        assert len(res.json()) == 3
        res = await client.get(
            f"{PREFIX}/activity/"
            f"?timestamp_started_min={quote(str(activity3.timestamp_started))}"
        )
        assert len(res.json()) == 2
        # combination and iconstains
        res = await client.get(f"{PREFIX}/activity/?status=OK&pkg_name=O")
        assert len(res.json()) == 2

    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)):
        res = await client.get(f"{PREFIX}/activity/")
        assert res.status_code == 200
        assert len(res.json()) == 0
