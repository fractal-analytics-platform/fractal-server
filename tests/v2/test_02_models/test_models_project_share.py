import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import ProjectPermissions


async def test_linkuserproject_constraints(
    db, tmp_path, local_resource_profile_db
):
    # Setup

    resource, profile = local_resource_profile_db

    u1 = UserOAuth(
        email="u1@example.org",
        hashed_password="hashed_password_1",
        project_dir="/fake",
    )
    u2 = UserOAuth(
        email="u2@example.org",
        hashed_password="hashed_password_2",
        project_dir="/fake",
    )

    p1 = ProjectV2(name="p1", resource_id=resource.id)
    p2 = ProjectV2(name="p2", resource_id=resource.id)

    db.add_all([u1, u2, p1, p2])
    await db.commit()

    await db.refresh(u1)
    await db.refresh(u2)
    await db.refresh(p1)
    await db.refresh(p2)

    await db.close()

    db.add(
        LinkUserProjectV2(
            project_id=p1.id,
            user_id=u1.id,
            is_owner=True,
            is_verified=True,
            permissions=ProjectPermissions.EXECUTE,
        )
    )
    await db.commit()

    # Test "ix_linkuserprojectv2_one_owner_per_project"
    db.add(
        LinkUserProjectV2(
            project_id=p1.id,
            user_id=u2.id,
            is_owner=True,
            is_verified=True,
            permissions=ProjectPermissions.EXECUTE,
        )
    )
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    assert "ix_linkuserprojectv2_one_owner_per_project" in e.value.args[0]
    await db.rollback()

    # Test "ck_linkuserprojectv2_owner_is_verified"
    db.add(
        LinkUserProjectV2(
            project_id=p2.id,
            user_id=u2.id,
            is_owner=True,
            is_verified=False,
            permissions=ProjectPermissions.EXECUTE,
        )
    )
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    assert "ck_linkuserprojectv2_owner_is_verified" in e.value.args[0]
    await db.rollback()

    # Test "ck_linkuserprojectv2_owner_full_permissions"
    for permissions in [ProjectPermissions.READ, ProjectPermissions.WRITE]:
        db.add(
            LinkUserProjectV2(
                project_id=p2.id,
                user_id=u2.id,
                is_owner=True,
                is_verified=True,
                permissions=permissions,
            )
        )
        with pytest.raises(IntegrityError) as e:
            await db.commit()
        assert "ck_linkuserprojectv2_owner_full_permissions" in e.value.args[0]
        await db.rollback()

    # Test "ck_linkuserprojectv2_valid_permissions"
    db.add(
        LinkUserProjectV2(
            project_id=p2.id,
            user_id=u1.id,
            is_owner=False,
            is_verified=True,
            permissions="rx",
        )
    )
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    assert "ck_linkuserprojectv2_valid_permissions" in e.value.args[0]
    await db.rollback()

    # OK
    db.add(
        LinkUserProjectV2(
            project_id=p2.id,
            user_id=u2.id,
            is_owner=True,
            is_verified=True,
            permissions=ProjectPermissions.EXECUTE,
        )
    )
    await db.commit()
