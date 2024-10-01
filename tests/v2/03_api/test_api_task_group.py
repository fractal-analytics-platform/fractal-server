from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.routes.auth._aux_auth import _get_default_user_group_id
from fractal_server.app.schemas.v2 import TaskGroupUpdateV2
from fractal_server.app.schemas.v2 import TaskReadV2

PREFIX = "/api/v2/task-group"


async def test_get_single_task_group(
    client,
    MockCurrentUser,
    task_factory_v2,
):
    async with MockCurrentUser() as user1:
        task = await task_factory_v2(user_id=user1.id, source="source")

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}")
        assert res.status_code == 200
        assert res.json()["user_id"] == user1.id
        assert len(res.json()["task_list"]) == 1
        assert res.json()["task_list"][0]["id"] == task.id

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id + 1}")
        assert res.status_code == 404

    async with MockCurrentUser():

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id + 1}")
        assert res.status_code == 404


def _ids_of_task_list(task_list: list[TaskReadV2]) -> list[int]:
    return [task["id"] for task in task_list]


async def test_get_task_group_list(
    client,
    MockCurrentUser,
    task_factory_v2,
    db,
):
    async with MockCurrentUser() as user1:
        task1 = await task_factory_v2(user_id=user1.id, source="source1")
        task2 = await task_factory_v2(user_id=user1.id, source="source2")

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        assert {
            *_ids_of_task_list(res.json()[0]["task_list"]),
            *_ids_of_task_list(res.json()[1]["task_list"]),
        } == {task1.id, task2.id}

    async with MockCurrentUser() as user2:
        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # Create a new UserGroup with user1 and user2
        new_group = UserGroup(name="new_group")
        db.add(new_group)
        await db.commit()
        await db.refresh(new_group)
        link1 = LinkUserGroup(user_id=user1.id, group_id=new_group.id)
        link2 = LinkUserGroup(user_id=user2.id, group_id=new_group.id)
        db.add(link1)
        db.add(link2)
        await db.commit()
        await db.close()

        task3 = await task_factory_v2(
            user_id=user2.id, user_group_id=new_group.id, source="source3"
        )

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == task3.id

    async with MockCurrentUser(user_kwargs={"id": user1.id}):

        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        assert {
            *_ids_of_task_list(res.json()[0]["task_list"]),
            *_ids_of_task_list(res.json()[1]["task_list"]),
            *_ids_of_task_list(res.json()[2]["task_list"]),
        } == {task1.id, task2.id, task3.id}


async def test_patch_task_group(
    client,
    MockCurrentUser,
    task_factory_v2,
    db,
):
    async with MockCurrentUser() as user1:
        task = await task_factory_v2(user_id=user1.id, source="source")

        res = await client.get(f"{PREFIX}/{task.taskgroupv2_id}")
        assert res.status_code == 200
        assert res.json()["user_id"] == user1.id
        assert res.json()["user_group_id"] is None

        default_user_group_id = await _get_default_user_group_id(db=db)

        # Update to `default_user_group_id`
        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}",
            json=TaskGroupUpdateV2(user_group_id=default_user_group_id).dict(),
        )
        assert res.status_code == 200
        assert res.json()["user_id"] == user1.id
        assert res.json()["user_group_id"] == default_user_group_id

        # Nothing to update
        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}",
            json=TaskGroupUpdateV2().dict(exclude_unset=True),
        )
        assert res.status_code == 422

        # TaskGroup does not exist
        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id + 1}",
            json=TaskGroupUpdateV2().dict(),
        )
        assert res.status_code == 404

        # UserGroup does not exist
        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}",
            json=TaskGroupUpdateV2(user_group_id=9999).dict(),
        )
        assert res.status_code == 404

        # Update to `None`
        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}",
            json=TaskGroupUpdateV2(user_group_id=None).dict(),
        )
        assert res.status_code == 200
        assert res.json()["user_id"] == user1.id
        assert res.json()["user_group_id"] is None

    async with MockCurrentUser():

        # Unauthorized
        res = await client.patch(
            f"{PREFIX}/{task.taskgroupv2_id}",
            json=TaskGroupUpdateV2(user_group_id=default_user_group_id).dict(),
        )
        assert res.status_code == 403
