import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user_group import UserGroupCreate
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.schemas.user_group import UserGroupUpdate


def test_user_create():

    u = UserCreate(email="a@b.c", password="asd")
    assert u.username is None

    u = UserCreate(email="a@b.c", password="pwd", username="username")
    assert u.username is not None


def test_user_group_create():
    UserGroupCreate(name="group1")
    with pytest.raises(ValidationError):
        UserGroupCreate()
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group1", something="else")


def test_user_group_update():
    g1 = UserGroupUpdate()
    assert g1.new_user_ids == []
    g2 = UserGroupUpdate(new_user_ids=[1])
    assert g2.new_user_ids == [1]
    g3 = UserGroupUpdate(new_user_ids=[1, 2])
    assert g3.new_user_ids == [1, 2]

    with pytest.raises(ValidationError):
        UserGroupUpdate(name="new name")
    with pytest.raises(ValidationError):
        UserGroupUpdate(name="new name", new_user_ids=[1, 2, 3])
    with pytest.raises(ValidationError):
        UserGroupUpdate(arbitrary_key="something")
    with pytest.raises(ValidationError):
        UserGroupUpdate(new_user_ids=["user@example.org"])
    with pytest.raises(ValidationError):
        UserGroupUpdate(new_user_ids=[dict(email="user@example.org")])


def test_user_group_read():
    from fractal_server.utils import get_timestamp

    XX = get_timestamp()
    g = UserGroupRead(id=1, name="group", timestamp_created=XX)
    assert g.user_ids is None
    g = UserGroupRead(id=1, name="group", timestamp_created=XX, user_ids=[])
    assert g.user_ids == []
    g = UserGroupRead(
        id=1, name="group", timestamp_created=XX, user_ids=[1, 2]
    )
    assert g.user_ids == [1, 2]
