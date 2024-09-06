import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserUpdate
from fractal_server.app.schemas.user import UserUpdateStrict


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
        UserCreate(email="a@b.c", password="asd", slurm_accounts=[1, "a", True])

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
        u = UserCreate(email="a@b.c", password="asd", cache_dir="  ")
    debug(e.value)
    assert "cannot be empty" in e.value.errors()[0]["msg"]
    # With invalid cache_dir attribute
    with pytest.raises(ValidationError) as e:
        u = UserCreate(email="a@b.c", password="asd", cache_dir="xxx")
    debug(e.value)
    assert "must be an absolute path" in e.value.errors()[0]["msg"]
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


def test_user_update_strict():
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=[42, "Foo"])
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=["Foo", True])
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts="NOT A LIST")
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=[{"NOT": "VALID"}])
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=["a", "b", "a"])
    UserUpdateStrict(slurm_accounts=None)
    UserUpdateStrict(slurm_accounts=["a", "b", "c"])


def test_user_group_create():
    g = UserGroupCreate(name="group1")
    assert g.user_ids == []

    g = UserGroupCreate(name="group1", user_ids=[1, 2, 3])

    with pytest.raises(ValidationError):
        UserGroupCreate(user_ids=[1, 2, 3])
    with pytest.raises(ValidationError):
        UserGroupCreate(name="group1", user_ids=[1, 2, 3], something="else")


def test_user_group_read():
    g = UserGroupRead(name="group")
    assert g.user_ids == []

    UserGroupRead(name="group", user_ids=[1, 2])

    with pytest.raises(ValidationError):
        UserGroupRead(name="group", user_ids=[1, 2], something="else")


def test_user_group_names_read():
    UserGroupNamesRead(names=["a", "b", "c"])


def test_user_group_update():
    # Successes

    g = UserGroupUpdate()
    assert g.new_user_ids == []

    UserGroupUpdate(new_user_ids=[1])

    UserGroupUpdate(new_user_ids=[1, 2, 3])

    # Failures

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
