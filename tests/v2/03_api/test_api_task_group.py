from urllib.parse import quote

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models.v2 import CollectionStateV2
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
            source="source",
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
    db,
):
    async with MockCurrentUser() as user1:
        await task_factory_v2(
            user_id=user1.id,
            source="source1",
            args_schema_non_parallel={"foo": 0, "bar": 1},
            args_schema_parallel={"xxx": 2, "yyy": 3},
        )
        await task_factory_v2(
            user_id=user1.id,
            source="source2",
            task_group_kwargs=dict(active=False),
            args_schema_non_parallel={"foo": 4, "bar": 5},
            args_schema_parallel={"xxx": 6, "yyy": 7},
        )

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        for group in res.json():
            for task in group["task_list"]:
                assert task["args_schema_non_parallel"] is not None
                assert task["args_schema_parallel"] is not None
        res = await client.get(f"{PREFIX}/?args_schema=false")
        assert res.status_code == 200
        assert len(res.json()) == 2
        for group in res.json():
            for task in group["task_list"]:
                assert task["args_schema_non_parallel"] is None
                assert task["args_schema_parallel"] is None

    async with MockCurrentUser() as user2:

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2

        # Create a new UserGroup with user2
        new_group = UserGroup(name="new_group")
        db.add(new_group)
        await db.commit()
        await db.refresh(new_group)
        link = LinkUserGroup(user_id=user2.id, group_id=new_group.id)
        db.add(link)
        await db.commit()
        await db.close()

        await task_factory_v2(
            user_id=user2.id,
            task_group_kwargs=dict(user_group_id=new_group.id),
            source="source3",
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

    async with MockCurrentUser(user_kwargs={"id": user1.id}):

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_delete_task_group(client, MockCurrentUser, task_factory_v2, db):
    async with MockCurrentUser() as user1:
        task = await task_factory_v2(user_id=user1.id, source="source")

    state = CollectionStateV2(taskgroupv2_id=task.taskgroupv2_id)
    task_group_activity = TaskGroupActivityV2(
        user_id=user1.id,
        taskgroupv2_id=task.taskgroupv2_id,
        pkg_name="pkg",
        version="1.0.0",
        action=TaskGroupActivityActionV2.COLLECT,
        status=TaskGroupActivityStatusV2.PENDING,
    )
    db.add(state)
    await db.commit()
    await db.refresh(state)
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    assert state.taskgroupv2_id == task.taskgroupv2_id
    assert task_group_activity.taskgroupv2_id == task.taskgroupv2_id

    async with MockCurrentUser():
        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 403

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 204
        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 404

    await db.refresh(state)
    assert state.taskgroupv2_id is None
    await db.refresh(task_group_activity)
    assert task_group_activity.taskgroupv2_id is None


async def test_delete_task_group_fail(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    workflowtask_factory_v2,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id, source="source")
        await workflowtask_factory_v2(workflow_id=workflow.id, task_id=task.id)

        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 422


async def test_patch_task_group(
    client,
    MockCurrentUser,
    task_factory_v2,
    default_user_group,
    user_group_factory,
    first_user,
):
    async with MockCurrentUser() as user1:
        taskA = await task_factory_v2(
            name="asd",
            user_id=user1.id,
            task_group_kwargs=dict(user_group_id=default_user_group.id),
        )
        group2 = await user_group_factory("team2", user1.id, first_user.id)
        taskB = await task_factory_v2(
            name="asd",
            user_id=first_user.id,
            task_group_kwargs=dict(user_group_id=group2.id),
        )

        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["active"] is True
        assert res.json()["user_group_id"] == default_user_group.id

        # Update 1: change `active`
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/", json=dict(active=False)
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["active"] is False
        assert res.json()["user_group_id"] == default_user_group.id

        # Update 2: change `active`
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/", json=dict(active=True)
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["active"] is True
        assert res.json()["user_group_id"] == default_user_group.id

        # Update 3: change both `user_group_id` and `active`
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/",
            json=dict(user_group_id=None, active=True),
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["active"] is True
        assert res.json()["user_group_id"] is None

        # Update 4: change both `user_group_id` and `active`
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/",
            json=dict(user_group_id=default_user_group.id, active=False),
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{taskA.taskgroupv2_id}/")
        assert res.json()["active"] is False
        assert res.json()["user_group_id"] == default_user_group.id

        # Non existing TaskGroup
        res = await client.patch(f"{PREFIX}/9999999/", json=dict(active=False))
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

    async with MockCurrentUser(user_kwargs=dict(id=first_user.id)):
        # Link the task-group to another usergroup and fail due to
        # non-duplication constraint
        res = await client.patch(
            f"{PREFIX}/{taskB.taskgroupv2_id}/",
            json=dict(user_group_id=default_user_group.id),
        )
        assert res.status_code == 422
        assert "already owns a task group" in res.json()["detail"]

    async with MockCurrentUser():

        # Unauthorized
        res = await client.patch(
            f"{PREFIX}/{taskA.taskgroupv2_id}/", json=dict(active=False)
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

        res = await client.get(f"{PREFIX}/activity/{activity.id+1}/")
        assert res.status_code == 404

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/activity/{activity.id}/")
        assert res.status_code == 403


async def test_get_task_group_activity_list(
    client, MockCurrentUser, db, task_factory_v2
):
    async with MockCurrentUser() as user:
        task = await task_factory_v2(user_id=user.id, source="source")

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

    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/activity/")
        assert res.status_code == 200
        assert len(res.json()) == 0
