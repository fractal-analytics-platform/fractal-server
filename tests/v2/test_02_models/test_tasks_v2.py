from devtools import debug
from sqlmodel import select
from sqlmodel import text

from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2


async def test_task_group_v2(db):
    user = UserOAuth(email="user@fractal.xy", hashed_password="1234")
    db.add(user)
    await db.commit()
    await db.refresh(user)

    user_group = UserGroup(name="group")
    db.add(user_group)
    await db.commit()
    await db.refresh(user_group)

    task1 = TaskV2(
        name="task1", type="parallel", command_parallel="cmd", source="xxx"
    )
    task2 = TaskV2(
        name="task2", type="parallel", command_parallel="cmd", source="yyy"
    )
    task3 = TaskV2(
        name="task3", type="parallel", command_parallel="cmd", source="zzz"
    )

    task_group = TaskGroupV2(
        user_id=user.id,
        active=True,
        task_list=[task1, task2, task3],
        origin="wheel-file",
        pkg_name="package-name",
    )
    db.add(task_group)
    await db.commit()

    await db.refresh(task_group)
    await db.refresh(task1)
    await db.refresh(task2)
    await db.refresh(task3)

    debug(task_group)

    assert len(task_group.task_list) == 3
    assert task1.taskgroupv2_id == task_group.id
    assert task2.taskgroupv2_id == task_group.id
    assert task3.taskgroupv2_id == task_group.id

    assert task_group.user_id == user.id
    assert task_group.user_group_id is None

    task_group.user_group_id = user_group.id
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    assert task_group.user_group_id == user_group.id

    # Delete user_group

    await db.delete(user_group)
    await db.commit()
    db.expunge_all()

    task_group = await db.get(TaskGroupV2, task_group.id)
    assert task_group.user_group_id is None

    # Consistency check

    task2 = await db.get(TaskV2, task2.id)
    await db.delete(task2)
    await db.commit()
    db.expunge_all()

    task_group = await db.get(TaskGroupV2, task_group.id)
    assert len(task_group.task_list) == 2

    # Test cascade on delete

    await db.delete(task_group)
    await db.commit()
    db.expunge_all()

    task_group = await db.get(TaskGroupV2, task_group.id)
    task1 = await db.get(TaskV2, task1.id)
    task3 = await db.get(TaskV2, task3.id)

    assert task_group is None
    assert task1 is None
    assert task3 is None


async def test_collection_state(db):

    user = UserOAuth(email="user@fractal.xy", hashed_password="1234")
    db.add(user)
    await db.commit()
    await db.refresh(user)

    task_group = TaskGroupV2(
        user_id=user.id,
        origin="wheel-file",
        pkg_name="package-name",
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)

    task_group_activity = TaskGroupActivityV2(
        user_id=user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action="collect",
        pkg_name="pkg",
        version="1.0.0",
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    assert task_group_activity.taskgroupv2_id == task_group.id

    await db.delete(task_group)
    await db.commit()
    db.expunge_all()

    task_group_activity = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    assert task_group_activity.taskgroupv2_id is None


async def test_non_nullable_json(task_factory_v2, db, MockCurrentUser):
    async with MockCurrentUser() as user:

        # Create TaskGroupV2 via factory
        await task_factory_v2(
            user_id=user.id,
            args_schema_parallel={"foo": "bar"},
            args_schema_non_parallel={"foo": "bar"},
        )
        res = await db.execute(select(TaskGroupV2))
        task_group = res.scalar()

        # Add a TaskV2 without any args_schemas
        task = TaskV2(
            name="foo",
            type="bar",
            taskgroupv2_id=task_group.id,
        )
        db.add(task)
        await db.commit()
        await db.refresh(task)

        # Assert that args_schemas are actully null
        assert task.args_schema_parallel is None
        assert task.args_schema_non_parallel is None

        # Assert that the query for SQL-null works
        res = await db.execute(
            text("SELECT * FROM taskv2 WHERE command_parallel IS NULL")
        )
        sql_null_command_parallel = res.fetchall()
        assert len(sql_null_command_parallel) == 1
        assert sql_null_command_parallel[0][0] == task.id

        # Assert that the query for SQL-null gives 0 results with JSONs
        res = await db.execute(
            text("SELECT * FROM taskv2 WHERE args_schema_parallel IS NULL")
        )
        sql_null = res.fetchall()
        assert len(sql_null) == 0

        # Assert that the query for JSON-null gives one result
        res = await db.execute(
            text(
                "SELECT * FROM taskv2 "
                "WHERE args_schema_parallel = 'null'::jsonb"
            )
        )
        json_null = res.fetchall()
        assert len(json_null) == 1
        assert json_null[0][0] == task.id
