import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models.v2.task_group import TaskGroupV2


async def add_taskgroup(
    db,
    user_id: int,
    resource_id: int,
    pkg_name: str,
    version: str | None = None,
    path: str | None = None,
    user_group_id: int | None = None,
):
    db.add(
        TaskGroupV2(
            user_id=user_id,
            user_group_id=user_group_id,
            pkg_name=pkg_name,
            version=version,
            resource_id=resource_id,
            path=path,
            task_list=[],
            origin="foo",
        )
    )
    await db.commit()


async def test_taskgroup_unique_contraints(
    db,
    MockCurrentUser,
    user_group_factory,
    local_resource_profile_db,
    slurm_sudo_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    slurm_resource, _ = slurm_sudo_resource_profile_db
    async with MockCurrentUser(profile_id=profile.id) as userA:
        userA_id = userA.id
    async with MockCurrentUser(profile_id=profile.id) as userB:
        userB_id = userB.id

    async with MockCurrentUser(profile_id=profile.id) as user:
        # 1) ix_taskgroupv2_user_unique_constraint
        await add_taskgroup(
            # constrained args
            user_id=user.id,
            pkg_name="pkg_name1",
            version=None,
            resource_id=resource.id,
            # other args
            db=db,
        )
        ## same -> fail
        with pytest.raises(IntegrityError):
            await add_taskgroup(
                user_id=user.id,
                pkg_name="pkg_name1",
                version=None,
                resource_id=resource.id,
                db=db,
            )
        await db.rollback()
        ## change `user_id` -> ok
        await add_taskgroup(
            user_id=userA_id,
            pkg_name="pkg_name1",
            version=None,
            resource_id=resource.id,
            db=db,
        )
        ## change `pkg_name` -> ok
        await add_taskgroup(
            user_id=user.id,
            pkg_name="pkg_name0",
            version=None,
            resource_id=resource.id,
            db=db,
        )
        ## change `version` -> ok
        await add_taskgroup(
            user_id=user.id,
            pkg_name="pkg_name1",
            version="foo",
            resource_id=resource.id,
            db=db,
        )
        ## change `resource_id` -> ok
        await add_taskgroup(
            user_id=user.id,
            pkg_name="pkg_name1",
            version=None,
            resource_id=slurm_resource.id,
            db=db,
        )

        # 2) ix_taskgroupv2_usergroup_unique_constraint
        group1 = await user_group_factory(
            "group1", user.id, userA_id, userB_id, db=db
        )
        group2 = await user_group_factory(
            "group2", user.id, userA_id, userB_id, db=db
        )
        group1_id, group2_id = group1.id, group2.id

        await add_taskgroup(
            # constrained args
            user_group_id=group1.id,
            pkg_name="pkg_name2",
            version=None,
            # other args
            user_id=user.id,
            resource_id=resource.id,
            db=db,
        )
        ## same -> fail
        with pytest.raises(IntegrityError):
            await add_taskgroup(
                user_group_id=group1_id,
                pkg_name="pkg_name2",
                version=None,
                user_id=user.id,
                resource_id=resource.id,
                db=db,
            )
        await db.rollback()

        ## change `user_group_id` -> ok
        await add_taskgroup(
            user_group_id=group2_id,
            pkg_name="pkg_name2",
            version=None,
            user_id=user.id,
            resource_id=slurm_resource.id,
            db=db,
        )
        ## change `pkg_name` -> ok
        await add_taskgroup(
            user_group_id=group1_id,
            pkg_name="new_pkg_name2",
            version=None,
            user_id=user.id,
            resource_id=slurm_resource.id,
            db=db,
        )
        ## change `version` -> ok
        await add_taskgroup(
            user_group_id=group1_id,
            pkg_name="pkg_name2",
            version="foo",
            user_id=user.id,
            resource_id=slurm_resource.id,
            db=db,
        )
        ## don't fail if `user_group_id` is None
        for u in [userA, userB, user]:
            await add_taskgroup(
                user_group_id=None,
                pkg_name="pkg_name",
                version=None,
                user_id=u.id,
                resource_id=slurm_resource.id,
                db=db,
            )

        # 3) ix_taskgroupv2_path_unique_constraint

        await add_taskgroup(
            # constrained args
            path="/path1",
            resource_id=resource.id,
            # other args
            pkg_name="pkg_name3",
            version="3",
            user_id=user.id,
            db=db,
        )
        ## same -> fail
        with pytest.raises(IntegrityError):
            await add_taskgroup(
                path="/path1",  # same
                resource_id=resource.id,  # same
                pkg_name="foo",
                version="bar",
                user_id=userA_id,
                db=db,
            )
        await db.rollback()
        ## change path -> ok
        await add_taskgroup(
            path="/path2",
            resource_id=resource.id,  # same
            pkg_name="foo",
            version="bar",
            user_id=userA_id,
            db=db,
        )
        ## change resource -> ok
        await add_taskgroup(
            path="/path1",  # same
            resource_id=slurm_resource.id,
            pkg_name="foo",
            version="bar",
            user_id=userA_id,
            db=db,
        )
        ## don't fail if `path` is None
        for u in [userA, userB, user]:
            await add_taskgroup(
                path=None,
                user_group_id=None,
                pkg_name="pkg_fancy_name",
                version=None,
                user_id=u.id,
                resource_id=slurm_resource.id,
                db=db,
            )
