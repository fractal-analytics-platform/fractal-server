import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import UserCreate


def test_user_create():
    # Without slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd")
    debug(u)
    assert u.slurm_user is None
    # With valid slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd", slurm_user="slurm_user")
    debug(u)
    assert u.slurm_user
    # With invalid slurm_user attribute
    with pytest.raises(ValidationError):
        u = UserCreate(email="a@b.c", password="asd", slurm_user="  ")
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
