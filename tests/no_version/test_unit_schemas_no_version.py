import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserUpdate
from fractal_server.app.schemas.user_group import UserGroupCreate
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.schemas.user_group import UserGroupUpdate


def test_user_create():
    # Without slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd")
    debug(u)
    assert u.slurm_user is None
    assert u.slurm_accounts == []
    # With valid slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd", slurm_user="slurm_user")
    assert u.slurm_user
    # With invalid slurm_user attribute
    with pytest.raises(ValidationError):
        UserCreate(email="a@b.c", password="asd", slurm_user="  ")

    # slurm_accounts must be a list of StrictStr without repetitions

    u = UserCreate(email="a@b.c", password="asd", slurm_accounts=["a", "b"])
    assert u.slurm_accounts == ["a", "b"]

    with pytest.raises(ValidationError):
        UserCreate(
            email="a@b.c", password="asd", slurm_accounts=[1, "a", True]
        )

    with pytest.raises(ValidationError):
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["a", {"NOT": "VALID"}],
        )
    with pytest.raises(ValidationError):
        # repetitions
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["foo", "bar", "foo", "rab"],
        )
    with pytest.raises(ValidationError):
        # empty string
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["foo", "     ", "bar"],
        )
    user = UserCreate(
        email="a@b.c",
        password="asd",
        slurm_accounts=["f o o", "  bar "],
    )
    assert user.slurm_accounts == ["f o o", "bar"]
    with pytest.raises(ValidationError):
        # repetition after stripping
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["   foo", "foo    "],
        )

    # With valid cache_dir
    CACHE_DIR = "/xxx"
    u = UserCreate(email="a@b.c", password="asd", cache_dir=f"{CACHE_DIR}   ")
    assert u.cache_dir == CACHE_DIR
    # With invalid cache_dir attribute
    with pytest.raises(ValidationError) as e:
        UserCreate(email="a@b.c", password="asd", cache_dir="  ")
    debug(e.value)
    assert "cannot be empty" in e.value.errors()[0]["msg"]
    # With invalid cache_dir attribute
    with pytest.raises(ValidationError) as e:
        UserCreate(email="a@b.c", password="asd", cache_dir="xxx")
    debug(e.value)
    assert "must be an absolute path" in e.value.errors()[0]["msg"]
    # With invalid cache_dir attribute
    with pytest.raises(
        ValidationError, match="must not contain any of this characters"
    ) as e:
        UserCreate(email="a@b.c", password="asd", cache_dir=f"{CACHE_DIR}*;")
    debug(e.value)

    # With all attributes
    u = UserCreate(
        email="a@b.c",
        password="pwd",
        slurm_user="slurm_user",
        username="username",
        cache_dir="/some/path",
    )
    debug(u)
    assert u.slurm_user
    assert u.cache_dir
    assert u.username
    with pytest.raises(ValidationError) as e:
        UserUpdate(cache_dir=None)


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
