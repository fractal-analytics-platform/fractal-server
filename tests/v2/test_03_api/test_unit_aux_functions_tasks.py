import pytest
from devtools import debug
from fastapi import HTTPException

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import TaskGroupActivityV2
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions_task_version_update import (  # noqa
    get_new_workflow_task_meta,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_collection_task_group_activity_status_message,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_full_access,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_read_access,
)
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME


async def test_get_task(db, task_factory_v2):
    # Create the following initial situations:
    # * User group A, with two users (A1 and A2)
    # * User B, who is not part of any group
    # * Task A_no_group, which belongs to user A1 and no group
    # * Task A_group_A, which belongs to user A1 and group A

    user_A1 = UserOAuth(email="a1@a.a", hashed_password="xxx")
    user_A2 = UserOAuth(email="a2@a.a", hashed_password="xxx")
    user_B = UserOAuth(email="b@b.b", hashed_password="xxx")
    group_0 = UserGroup(name=FRACTAL_DEFAULT_GROUP_NAME)
    group_A = UserGroup(name="A")
    db.add(user_A1)
    db.add(user_A2)
    db.add(user_B)
    db.add(group_0)
    db.add(group_A)
    await db.commit()
    await db.refresh(user_A1)
    await db.refresh(user_A2)
    await db.refresh(user_B)
    await db.refresh(group_0)
    await db.refresh(group_A)
    db.add(LinkUserGroup(user_id=user_A1.id, group_id=group_0.id))
    db.add(LinkUserGroup(user_id=user_A1.id, group_id=group_A.id))
    db.add(LinkUserGroup(user_id=user_A2.id, group_id=group_0.id))
    db.add(LinkUserGroup(user_id=user_A2.id, group_id=group_A.id))
    await db.commit()

    task_A_no_group = await task_factory_v2(user_id=user_A1.id, source="1")
    task_A_group_A = await task_factory_v2(
        user_id=user_A1.id,
        task_group_kwargs=dict(user_group_id=group_A.id),
        source="2",
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
    """
    Test the `require_active` argument of `_get_task_read_access`.
    """
    # Preliminary setup
    user = UserOAuth(email="a@a.a", hashed_password="xxx")
    db.add(user)
    await db.commit()
    await db.refresh(user)
    group_0 = UserGroup(name=FRACTAL_DEFAULT_GROUP_NAME)
    db.add(group_0)
    await db.commit()
    await db.refresh(group_0)
    db.add(LinkUserGroup(user_id=user.id, group_id=group_0.id))
    await db.commit()

    task = await task_factory_v2(user_id=user.id, source="1")
    task_group = await db.get(TaskGroupV2, task.taskgroupv2_id)

    # Make sure task group is active, and verify access is always OK
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

    # Make sure task group is not active, and verify access depends on
    # `require_active`
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


async def test_get_collection_task_group_activity_status_message(
    db,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        # Create task group
        task_group = TaskGroupV2(
            user_id=user.id,
            origin="other",
            pkg_name="pkg_name",
            version="version",
        )
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)

        # Add one non-collection activity
        db.add(
            TaskGroupActivityV2(
                user_id=user.id,
                taskgroupv2_id=task_group.id,
                pkg_name=task_group.pkg_name,
                version=task_group.version,
                status="OK",
                action="deactivate",
            )
        )
        await db.commit()

        # Check message
        msg = await _get_collection_task_group_activity_status_message(
            task_group_id=task_group.id,
            db=db,
        )
        debug(msg)
        assert msg == ""

        # Add one collection activity
        db.add(
            TaskGroupActivityV2(
                user_id=user.id,
                taskgroupv2_id=task_group.id,
                pkg_name=task_group.pkg_name,
                version=task_group.version,
                status="OK",
                action="collect",
            )
        )
        await db.commit()

        # Check message
        msg = await _get_collection_task_group_activity_status_message(
            task_group_id=task_group.id,
            db=db,
        )
        debug(msg)
        assert "There exists another task-group collection" in msg

        # Add another collection activity
        db.add(
            TaskGroupActivityV2(
                user_id=user.id,
                taskgroupv2_id=task_group.id,
                pkg_name=task_group.pkg_name,
                version=task_group.version,
                status="OK",
                action="collect",
            )
        )
        await db.commit()

        # Check message
        msg = await _get_collection_task_group_activity_status_message(
            task_group_id=task_group.id,
            db=db,
        )
        debug(msg)
        assert "please contact an admin" in msg


def test_get_new_workflow_task_meta():

    assert get_new_workflow_task_meta(
        old_task_meta=None,
        old_workflow_task_meta={"foo": "bar"},
        new_task_meta=None,
    ) == {"foo": "bar"}

    assert get_new_workflow_task_meta(
        old_task_meta={"foo": "bar"},
        old_workflow_task_meta=None,
        new_task_meta={"bar": "foo"},
    ) == {"bar": "foo"}

    assert get_new_workflow_task_meta(
        old_task_meta={"mem": 6000, "cpus_per_task": 1, "needs_gpu": True},
        old_workflow_task_meta={"mem": 6000, "cpus_per_task": 2},
        new_task_meta={"needs_luck": True},
    ) == {"cpus_per_task": 2, "needs_luck": True}

    assert get_new_workflow_task_meta(
        old_task_meta={"mem": 6000, "cpus_per_task": 1, "needs_gpu": True},
        old_workflow_task_meta={"mem": 6000, "cpus_per_task": 2},
        new_task_meta=None,
    ) == {"cpus_per_task": 2}
