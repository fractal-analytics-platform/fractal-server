import pytest
from fastapi.exceptions import HTTPException

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_group_read_access,
)


async def test_get_task_group_read_access(
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    db,
    client,
    local_resource_profile_db,
    slurm_sudo_resource_profile_db,
):
    """
    Test the loss-of-access-to-task scenario described in
    https://github.com/fractal-analytics-platform/fractal-server/issues/1840
    """
    resource_local, profile_local = local_resource_profile_db
    resource_slurm_sudo, profile_slurm_sudo = slurm_sudo_resource_profile_db
    # Create user and user group
    # Users A and B belong to the same profile/resource
    # User C is on a different one
    user_A = UserOAuth(
        hashed_password="x",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        profile_id=profile_local.id,
        email="userA@example.org",
        project_dir="/fake",
    )
    user_B = UserOAuth(
        hashed_password="x",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        profile_id=profile_local.id,
        email="userB@example.org",
        project_dir="/fake",
    )
    user_C = UserOAuth(
        hashed_password="x",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        profile_id=profile_slurm_sudo.id,
        email="userC@example.org",
        project_dir="/fake",
    )
    user_group = UserGroup(name="team")
    db.add(user_A)
    db.add(user_B)
    db.add(user_C)
    db.add(user_group)
    await db.commit()
    await db.refresh(user_A)
    await db.refresh(user_B)
    await db.refresh(user_C)
    await db.refresh(user_group)
    db.add(LinkUserGroup(user_id=user_A.id, group_id=user_group.id))
    db.add(LinkUserGroup(user_id=user_B.id, group_id=user_group.id))
    db.add(LinkUserGroup(user_id=user_C.id, group_id=user_group.id))
    await db.commit()

    task_group = TaskGroupV2(
        user_id=user_A.id,
        user_group_id=user_group.id,
        resource_id=resource_local.id,
        pkg_name="x",
        version="1",
        origin="other",
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)

    # User A has access, since they are the owner
    await _get_task_group_read_access(
        task_group_id=task_group.id,
        user_id=user_A.id,
        db=db,
    )

    # User B has access, because they are in the same user group _and_ they
    # are associated to the same resource
    await _get_task_group_read_access(
        task_group_id=task_group.id,
        user_id=user_B.id,
        db=db,
    )
    # User C does not have access, because they are in the same user group but
    # they are associated to a different resource
    with pytest.raises(
        HTTPException,
        match="Current user has no read access",
    ):
        await _get_task_group_read_access(
            task_group_id=task_group.id,
            user_id=user_C.id,
            db=db,
        )
