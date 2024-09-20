from devtools import debug

from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings


async def test_unit_link_user_to_settings(db):
    # User with no settings
    user_A = UserOAuth(
        email="a@a.a",
        hashed_password="xxx",
        is_active=True,
        is_superuser=False,
        is_verified=True,
    )
    db.add(user_A)
    await db.commit()
    await db.refresh(user_A)
    debug(user_A)
    assert user_A.settings is None
    assert user_A.user_settings_id is None

    # User associated to settings after its initial creation
    user_settings_1 = UserSettings(ssh_host="127.0.0.1")
    db.add(user_settings_1)
    await db.commit()
    await db.refresh(user_settings_1)
    debug(user_settings_1)

    user_B = UserOAuth(
        email="b@b.b",
        hashed_password="xxx",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        user_settings_id=user_settings_1.id,
    )
    db.add(user_B)
    await db.commit()
    await db.refresh(user_B)
    debug(user_B)

    assert user_B.settings is not None
    assert user_B.user_settings_id is not None

    # User associated to settings upon its initial creation
    user_settings_2 = UserSettings(ssh_host="127.0.0.1")
    user_C = UserOAuth(
        email="c@c.c",
        hashed_password="xxx",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        settings=user_settings_2,
    )
    db.add(user_C)
    await db.commit()
    await db.refresh(user_C)
    debug(user_C)
    assert user_C.settings is not None
    assert user_C.user_settings_id is not None

    # User associated to settings during its initial creation / second version
    user_D = UserOAuth(
        email="d@d.d",
        hashed_password="xxx",
        is_active=True,
        is_superuser=False,
        is_verified=True,
    )
    db.add(user_D)
    await db.commit()
    await db.refresh(user_D)
    user_D.settings = UserSettings()
    await db.merge(user_D)
    await db.commit()
    await db.refresh(user_D)
    debug(user_D)
    assert user_D.user_settings_id is not None
    assert user_D.settings is not None

    # FIXME: Test delete cascade
