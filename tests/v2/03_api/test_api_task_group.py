from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup

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
            user_id=user1.id, user_group_id=new_group.id, source="source"
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
        await task_factory_v2(user_id=user1.id, source="source1")
        await task_factory_v2(user_id=user1.id, source="source2")

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2

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
            user_id=user2.id, user_group_id=new_group.id, source="source3"
        )

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 3

    async with MockCurrentUser(user_kwargs={"id": user1.id}):

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_delete_task_group(client, MockCurrentUser, task_factory_v2):
    async with MockCurrentUser() as user1:
        task = await task_factory_v2(user_id=user1.id, source="source")

    async with MockCurrentUser():
        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 403

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 204
        res = await client.delete(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.status_code == 404


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


async def test_patch_task_group(client, MockCurrentUser, task_factory_v2):
    async with MockCurrentUser() as user1:

        task = await task_factory_v2(user_id=user1.id, source="source")

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.json()["active"] is True

        # Update

        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}/", json=dict(active=False)
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}/")
        assert res.json()["active"] is False

        # Non existing TaskGroup

        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id+1}/", json=dict(active=False)
        )
        assert res.status_code == 404

        # Non existing UserGroup

        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}/", json=dict(user_group_id=42)
        )
        assert res.status_code == 404

    async with MockCurrentUser():

        # Unauthorized

        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}/", json=dict(active=False)
        )
        assert res.status_code == 403
