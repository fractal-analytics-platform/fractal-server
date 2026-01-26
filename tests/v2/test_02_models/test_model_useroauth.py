import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models import UserOAuth


async def test_useroauth_constraints(db):
    u1 = UserOAuth(
        email="u1@example.org",
        hashed_password="1234",
        project_dirs=[],
        is_superuser=True,
    )
    assert u1.is_guest is None
    db.add(u1)
    await db.commit()
    await db.refresh(u1)
    assert u1.is_guest is False

    u2 = UserOAuth(
        email="u1@example.org",
        hashed_password="1234",
        project_dirs=[],
        is_superuser=True,
        is_guest=True,
    )
    db.add(u2)
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    await db.rollback()
    assert "superuser_is_not_guest" in e.value.args[0]
