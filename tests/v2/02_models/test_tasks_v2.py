import pytest
from devtools import debug
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


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
        user_id=user.id, active=True, task_list=[task1, task2, task3]
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

    settings = Inject(get_settings)
    if settings.DB_ENGINE == "sqlite":

        await db.delete(user_group)
        await db.commit()
        await db.refresh(task_group)

        assert task_group.user_group_id is not None  # SQLite and FK problem

        task_group.user_group_id = None
        db.add(task_group)
        await db.commit()

    else:

        await db.delete(user_group)
        with pytest.raises(IntegrityError):
            # Fail because `task_group.user_group_id` is not None
            await db.commit()
        await db.rollback()

        await db.delete(user_group)
        task_group.user_group_id = None
        db.add(task_group)
        await db.commit()

        await db.refresh(task_group)
        assert task_group.user_group_id is None

    # Consistency check

    await db.delete(task2)
    await db.commit()
    await db.refresh(task_group)

    assert len(task_group.task_list) == 2

    # Test cascade on delete

    await db.delete(task_group)
    await db.commit()

    task_group = await db.get(TaskGroupV2, task_group.id)
    task1 = await db.get(TaskV2, task1.id)
    task3 = await db.get(TaskV2, task3.id)

    assert task_group is None
    assert task1 is None
    assert task3 is None
