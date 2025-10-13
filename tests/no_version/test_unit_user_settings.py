import pytest
from devtools import debug
from fastapi import HTTPException

from fractal_server.app.models import UserOAuth
from fractal_server.app.models import UserSettings
from fractal_server.app.routes.aux.validate_user_settings import (
    validate_user_settings,
)
from fractal_server.app.schemas.v2 import ResourceType


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


async def test_validate_user_settings(db):
    common_attributes = dict(
        hashed_password="xxx",
        is_active=True,
        is_superuser=False,
        is_verified=True,
    )

    # Prepare users
    user_without_settings = UserOAuth(email="a@a.a", **common_attributes)
    db.add(user_without_settings)
    await db.commit()
    await db.refresh(user_without_settings)

    invalid_settings = UserSettings()
    user_with_invalid_settings = UserOAuth(
        email="b@b.b",
        **common_attributes,
        settings=invalid_settings,
    )
    db.add(user_with_invalid_settings)
    await db.commit()
    await db.refresh(user_with_invalid_settings)

    valid_settings = UserSettings(project_dir="/example/project")
    user_with_valid_ssh_settings = UserOAuth(
        email="c@c.c",
        **common_attributes,
        settings=valid_settings,
    )
    db.add(user_with_valid_ssh_settings)
    await db.commit()
    await db.refresh(user_with_valid_ssh_settings)

    # User with no settings
    with pytest.raises(HTTPException, match="has no settings"):
        await validate_user_settings(
            user=user_without_settings, backend="slurm_ssh", db=db
        )

    # User with empty settings: backend="local"
    await validate_user_settings(
        user=user_with_invalid_settings, backend=ResourceType.LOCAL, db=db
    )
    # User with empty settings: backend="slurm_ssh"
    with pytest.raises(HTTPException, match="SlurmSshUserSettings"):
        await validate_user_settings(
            user=user_with_invalid_settings, backend="slurm_ssh", db=db
        )
    # User with empty settings: backend="slurm_sudo"
    with pytest.raises(HTTPException, match="SlurmSudoUserSettings"):
        await validate_user_settings(
            user=user_with_invalid_settings, backend="slurm_sudo", db=db
        )

    # User with valid SSH settings: backend="slurm_ssh"
    await validate_user_settings(
        user=user_with_valid_ssh_settings, backend="slurm_ssh", db=db
    )
    # User with valid SSH settings: backend="slurm_sudo"
    await validate_user_settings(
        user=user_with_valid_ssh_settings, backend="slurm_sudo", db=db
    )
