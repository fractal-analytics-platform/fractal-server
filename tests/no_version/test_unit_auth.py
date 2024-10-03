import pytest
from devtools import debug
from fastapi import HTTPException
from sqlmodel import select

from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth._aux_auth import (
    _get_single_group_with_user_ids,
)
from fractal_server.app.routes.auth._aux_auth import (
    _get_single_user_with_group_ids,
)
from fractal_server.app.routes.auth._aux_auth import (
    _get_single_user_with_group_names,
)
from fractal_server.app.routes.auth._aux_auth import _user_or_404
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.security import _create_first_group  # noqa
from fractal_server.app.security import _create_first_user


async def _get_first_user(db):
    stm = select(UserOAuth)
    res = await db.execute(stm)
    return res.scalars().unique().all()[0]


async def test_user_or_404(db):
    with pytest.raises(HTTPException) as exc_info:
        await _user_or_404(user_id=9999, db=db)
    debug(exc_info.value)
    await _create_first_user(email="test1@fractal.com", password="xxxx")
    user = await _get_first_user(db)
    await _user_or_404(user_id=user.id, db=db)


async def test_get_single_group_with_user_ids(db):
    with pytest.raises(HTTPException) as exc_info:
        await _get_single_group_with_user_ids(group_id=9999, db=db)
    debug(exc_info.value)


async def test_get_single_user_with_group_names(db):
    await _create_first_user(email="test1@fractal.com", password="xxxx")
    user = await _get_first_user(db)
    res = await _get_single_user_with_group_names(user=user, db=db)
    debug(res)


async def test_get_single_user_with_group_ids(db):
    await _create_first_user(email="test1@fractal.com", password="xxxx")
    user = await _get_first_user(db)
    res = await _get_single_user_with_group_ids(user=user, db=db)
    debug(res)


async def test_verify_user_belongs_to_group(db):
    with pytest.raises(HTTPException) as exc_info:
        await _verify_user_belongs_to_group(user_id=1, user_group_id=42, db=db)
    debug(exc_info.value)
