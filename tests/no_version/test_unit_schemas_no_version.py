import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user_group import UserGroupCreate
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.types.validators import val_absolute_path
from fractal_server.types.validators import val_s3_url


def test_user_create():
    u = UserCreate(email="a@b.c", password="asd", project_dirs=["/fake"])
    assert u.slurm_accounts == []


def test_user_group_create():
    ug = UserGroupCreate(name="group")
    assert ug.name == "group"
    with pytest.raises(ValidationError):
        UserGroupCreate()
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group1", something="else")


def test_user_group_read():
    from fractal_server.utils import get_timestamp

    XX = get_timestamp()
    g = UserGroupRead(id=1, name="group", timestamp_created=XX)
    assert g.user_ids is None
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        user_ids=[],
    )
    assert g.user_ids == []
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        user_ids=[1, 2],
    )
    assert g.user_ids == [1, 2]


def test_unit_val_absolute_path():
    val_absolute_path("/path")
    with pytest.raises(ValueError):
        val_absolute_path("   ")
    with pytest.raises(ValueError):
        val_absolute_path("non/absolute/path")


def test_unit_val_s3_url():
    val_s3_url("s3://bucket/key")
    val_s3_url("s3://bucket/key/with/more/paths")
    with pytest.raises(ValueError):
        val_s3_url("s3://")
    with pytest.raises(ValueError):
        val_s3_url("s3://bucket")
    with pytest.raises(ValueError):
        val_s3_url("s3://bucket/ key")
    with pytest.raises(ValueError):
        val_s3_url("s3:/missing-slashes")
    with pytest.raises(ValueError):
        val_s3_url("http://not-an-s3-url")
    with pytest.raises(ValueError):
        val_s3_url("/also-not-an-s3-url")
