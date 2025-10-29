import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user_group import UserGroupCreate
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.schemas.user_group import UserGroupUpdate
from fractal_server.types.validators import val_absolute_path


def test_user_create():
    u = UserCreate(email="a@b.c", password="asd", project_dir="/fake")
    assert u.slurm_accounts == []


def test_user_group_create():
    ug = UserGroupCreate(name="group1")
    assert ug.viewer_paths == []
    ug = UserGroupCreate(name="group1", viewer_paths=["/a", "/b"])
    assert ug.viewer_paths == ["/a", "/b"]
    with pytest.raises(ValidationError):
        UserGroupCreate()
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group1", something="else")

    with pytest.raises(ValidationError):
        UserGroupCreate(name="group2", viewer_paths=None)
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group2", viewer_paths=[""])
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group2", viewer_paths=[" "])
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group2", viewer_paths=["/repeated", "/repeated"])
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group2", viewer_paths=["non/absolute"])


def test_user_group_update():
    g1 = UserGroupUpdate()
    assert g1.viewer_paths is None
    g4 = UserGroupUpdate(viewer_paths=["/a", "/b", "/c"])
    assert g4.viewer_paths == ["/a", "/b", "/c"]

    with pytest.raises(ValidationError):
        UserGroupUpdate(arbitrary_key="something")
    with pytest.raises(ValidationError):
        UserGroupUpdate(viewer_paths=None)
    with pytest.raises(ValidationError):
        UserGroupUpdate(viewer_paths=[""])
    with pytest.raises(ValidationError):
        UserGroupUpdate(viewer_paths=[" "])
    with pytest.raises(ValidationError):
        UserGroupUpdate(viewer_paths=["/repeated", "/repeated"])
    with pytest.raises(ValidationError):
        UserGroupUpdate(viewer_paths=["non/absolute"])


def test_user_group_read():
    from fractal_server.utils import get_timestamp

    XX = get_timestamp()
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        viewer_paths=[],
        resource_id=1,
    )
    assert g.user_ids is None
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        user_ids=[],
        viewer_paths=["/a"],
        resource_id=None,
    )
    assert g.user_ids == []
    assert g.viewer_paths == ["/a"]
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        user_ids=[1, 2],
        viewer_paths=[],
        resource_id=1,
    )
    assert g.user_ids == [1, 2]


def test_unit_val_absolute_path():
    val_absolute_path("/path")
    with pytest.raises(ValueError):
        val_absolute_path("   ")
    with pytest.raises(ValueError):
        val_absolute_path("non/absolute/path")
