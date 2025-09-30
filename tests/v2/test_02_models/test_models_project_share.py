import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2


async def test_linkuserproject_constraints(db):
    # Setup

    u1 = UserOAuth(email="u1@xy.z", hashed_password="hashed_password_1")
    u2 = UserOAuth(email="u2@xy.z", hashed_password="hashed_password_2")
    u3 = UserOAuth(email="u3@xy.z", hashed_password="hashed_password_3")

    p1 = ProjectV2(name="p1")
    p2 = ProjectV2(name="p2")
    p3 = ProjectV2(name="p3")

    db.add_all([u1, u2, u3, p1, p2, p3])
    await db.commit()

    await db.refresh(u1)
    await db.refresh(u2)
    await db.refresh(u3)
    await db.refresh(p1)
    await db.refresh(p2)
    await db.refresh(p3)

    await db.close()

    db.add(LinkUserProjectV2(project_id=p1.id, user_id=u1.id, is_owner=True))
    await db.commit()

    # Primary key constraint

    # * OK
    db.add(LinkUserProjectV2(project_id=p2.id, user_id=u1.id, is_owner=False))
    await db.commit()

    # * FAIL
    with pytest.raises(IntegrityError) as e:
        db.add(
            LinkUserProjectV2(project_id=p1.id, user_id=u1.id, is_owner=False)
        )
        await db.commit()
    assert "linkuserprojectv2_pkey" in e.value.args[0]
    await db.rollback()

    # Only one owner per project constraint

    # * OK
    db.add(LinkUserProjectV2(project_id=p1.id, user_id=u2.id, is_owner=False))
    await db.commit()
    # * FAIL
    with pytest.raises(IntegrityError) as e:
        db.add(
            LinkUserProjectV2(project_id=p1.id, user_id=u3.id, is_owner=True)
        )
        await db.commit()
    assert "idx_max_one_owner_per_project" in e.value.args[0]
    await db.rollback()

    # CHECK (not (is_owner and not verified))
    common = dict(project_id=p2.id, user_id=u2.id, is_owner=True)
    # * FAIL
    with pytest.raises(IntegrityError) as e:
        db.add(LinkUserProjectV2(**common, is_verified=False))
        await db.commit()
    assert "chk_owner_must_be_verified" in e.value.args[0]
    await db.rollback()
    # * OK
    db.add(LinkUserProjectV2(**common, is_verified=True))
    await db.commit()

    # CHECK (not (execute and not write))
    common = dict(project_id=p3.id, user_id=u1.id, can_execute=True)
    # * FAIL
    with pytest.raises(IntegrityError) as e:
        db.add(LinkUserProjectV2(**common, can_write=False))
        await db.commit()
    assert "chk_execute_implies_write" in e.value.args[0]
    await db.rollback()
    # * OK
    db.add(LinkUserProjectV2(**common, can_write=True))
    await db.commit()
