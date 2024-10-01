import pytest
from fastapi import HTTPException

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_full_access,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_read_access,
)


async def test_get_task(db, task_factory_v2):

    # Create the following initial situations:
    # * User group A, with two users (A1 and A2)
    # * User B, who is not part of any group
    # * Task A_no_group, which belongs to user A1 and no group
    # * Task A_group_A, which belongs to user A1 and group A

    user_A1 = UserOAuth(email="a1@a.a", hashed_password="xxx")
    user_A2 = UserOAuth(email="a2@a.a", hashed_password="xxx")
    user_B = UserOAuth(email="b@b.b", hashed_password="xxx")
    group_A = UserGroup(name="A")
    db.add(user_A1)
    db.add(user_A2)
    db.add(user_B)
    db.add(group_A)
    await db.commit()
    await db.refresh(user_A1)
    await db.refresh(user_A2)
    await db.refresh(user_B)
    await db.refresh(group_A)
    db.add(LinkUserGroup(user_id=user_A1.id, group_id=group_A.id))
    db.add(LinkUserGroup(user_id=user_A2.id, group_id=group_A.id))
    await db.commit()
    task_A_no_group = await task_factory_v2(
        user_id=user_A1.id, user_group_id=None, source="1"
    )
    task_A_group_A = await task_factory_v2(
        user_id=user_A1.id, user_group_id=group_A.id, source="2"
    )

    # Existence check success
    await _get_task_or_404(task_id=task_A_no_group.id, db=db)
    await _get_task_or_404(task_id=task_A_group_A.id, db=db)

    # Existence check failure
    with pytest.raises(HTTPException, match="404"):
        await _get_task_or_404(task_id=99999, db=db)

    # Read access success (user is task-group owner)
    await _get_task_read_access(
        task_id=task_A_no_group.id, db=db, user_id=user_A1.id
    )
    await _get_task_read_access(
        task_id=task_A_group_A.id, db=db, user_id=user_A1.id
    )

    # Read access success (user belongs to user group of task-group)
    await _get_task_read_access(
        task_id=task_A_group_A.id, db=db, user_id=user_A2.id
    )

    # Read access failures
    with pytest.raises(HTTPException, match="403"):
        await _get_task_read_access(
            task_id=task_A_no_group.id, db=db, user_id=user_A2.id
        )
    with pytest.raises(HTTPException, match="403"):
        await _get_task_read_access(
            task_id=task_A_no_group.id, db=db, user_id=user_B.id
        )
    with pytest.raises(HTTPException, match="403"):
        await _get_task_read_access(
            task_id=task_A_group_A.id, db=db, user_id=user_B.id
        )

    # Full access success (user is task-group owner)
    await _get_task_full_access(
        task_id=task_A_no_group.id, db=db, user_id=user_A1.id
    )
    await _get_task_full_access(
        task_id=task_A_group_A.id, db=db, user_id=user_A1.id
    )

    # Full access failures
    with pytest.raises(HTTPException, match="403"):
        await _get_task_full_access(
            task_id=task_A_group_A.id, db=db, user_id=user_A2.id
        )
    with pytest.raises(HTTPException, match="403"):
        await _get_task_full_access(
            task_id=task_A_no_group.id, db=db, user_id=user_A2.id
        )
    with pytest.raises(HTTPException, match="403"):
        await _get_task_full_access(
            task_id=task_A_no_group.id, db=db, user_id=user_B.id
        )
    with pytest.raises(HTTPException, match="403"):
        await _get_task_full_access(
            task_id=task_A_group_A.id, db=db, user_id=user_B.id
        )


async def test_get_task_require_active(db, task_factory_v2):

    user = UserOAuth(email="a@a.a", hashed_password="xxx")
    db.add(user)
    await db.commit()
    await db.refresh(user)
    task = await task_factory_v2(
        user_id=user.id, user_group_id=None, source="1"
    )
    task_group = await db.get(TaskGroupV2, task.taskgroupv2_id)

    task_group.active = True
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)

    await _get_task_read_access(
        task_id=task.id, user_id=user.id, db=db, require_active=False
    )
    await _get_task_read_access(
        task_id=task.id, user_id=user.id, db=db, require_active=True
    )

    task_group.active = False
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)

    await _get_task_read_access(
        task_id=task.id, user_id=user.id, db=db, require_active=False
    )
    with pytest.raises(HTTPException, match="422"):
        await _get_task_read_access(
            task_id=task.id, user_id=user.id, db=db, require_active=True
        )
