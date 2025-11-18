import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2


async def test_linkuserproject_constraints(
    db, tmp_path, local_resource_profile_db
):
    # Setup

    resource, profile = local_resource_profile_db

    u1 = UserOAuth(
        email="u1@xy.z",
        hashed_password="hashed_password_1",
        project_dir=(tmp_path / "p1").as_posix(),
    )
    u2 = UserOAuth(
        email="u2@xy.z",
        hashed_password="hashed_password_2",
        project_dir=(tmp_path / "p2").as_posix(),
    )
    u3 = UserOAuth(
        email="u3@xy.z",
        hashed_password="hashed_password_3",
        project_dir=(tmp_path / "p3").as_posix(),
    )

    p1 = ProjectV2(name="p1", resource_id=resource.id)
    p2 = ProjectV2(name="p2", resource_id=resource.id)

    db.add_all([u1, u2, u3, p1, p2])
    await db.commit()

    await db.refresh(u1)
    await db.refresh(u2)
    await db.refresh(u3)
    await db.refresh(p1)
    await db.refresh(p2)

    await db.close()

    db.add(
        LinkUserProjectV2(
            project_id=p1.id,
            user_id=u1.id,
            is_owner=True,
            is_verified=True,
            permissions="rwx",
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
            permissions="rwx",
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
            permissions="rwx",
        )
    )
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    assert "ck_linkuserprojectv2_owner_is_verified" in e.value.args[0]
    await db.rollback()

    # Test "ck_linkuserprojectv2_owner_full_permissions"
    for permissions in ["r", "rw"]:
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
            user_id=u3.id,
            is_owner=False,
            is_verified=True,
            permissions="foo",
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
            permissions="rwx",
        )
    )
    await db.commit()
