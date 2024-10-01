import pytest
from fastapi import HTTPException

from fractal_server.app.models import LinkUserGroup
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

    # Create two users
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

    # Existence check
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
