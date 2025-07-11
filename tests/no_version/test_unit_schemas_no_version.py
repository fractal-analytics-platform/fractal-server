import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user_group import UserGroupCreate
from fractal_server.app.schemas.user_group import UserGroupRead
from fractal_server.app.schemas.user_group import UserGroupUpdate
from fractal_server.app.schemas.user_settings import UserSettingsRead
from fractal_server.app.schemas.user_settings import UserSettingsReadStrict
from fractal_server.app.schemas.user_settings import UserSettingsUpdate
from fractal_server.app.schemas.user_settings import UserSettingsUpdateStrict
from fractal_server.types.validators import val_absolute_path


def test_user_create():
    u = UserCreate(email="a@b.c", password="asd")
    assert u.username is None

    u = UserCreate(email="a@b.c", password="pwd", username="username")
    assert u.username is not None


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
        id=1, name="group", timestamp_created=XX, viewer_paths=[]
    )
    assert g.user_ids is None
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        user_ids=[],
        viewer_paths=["/a"],
    )
    assert g.user_ids == []
    assert g.viewer_paths == ["/a"]
    g = UserGroupRead(
        id=1,
        name="group",
        timestamp_created=XX,
        user_ids=[1, 2],
        viewer_paths=[],
    )
    assert g.user_ids == [1, 2]


def test_user_settings_read():
    data = dict(
        id=1,
        ssh_host="MY_HOST",
        ssh_username="MY_SSH_USERNAME",
        slurm_accounts=[],
    )
    read = UserSettingsRead(**data)
    assert read.model_dump().get("ssh_host") == "MY_HOST"
    read_strict = UserSettingsReadStrict(**data)
    assert read_strict.model_dump().get("ssh_host") is None
    assert read_strict.model_dump().get("ssh_username") == "MY_SSH_USERNAME"


def test_user_settings_update():
    update_request_body = UserSettingsUpdate(ssh_host="NEW_HOST")
    assert update_request_body.slurm_accounts is None

    update_request_body = UserSettingsUpdate(
        ssh_host="NEW_HOST", slurm_accounts=None
    )
    assert update_request_body.slurm_accounts is None

    with pytest.raises(ValidationError):
        UserSettingsUpdate(slurm_accounts=[""])
    with pytest.raises(ValidationError):
        UserSettingsUpdate(slurm_accounts=["a", "a"])
    with pytest.raises(ValidationError):
        UserSettingsUpdate(slurm_accounts=["a", "a "])
    with pytest.raises(ValidationError):
        UserSettingsUpdateStrict(ssh_host="NEW_HOST")
    with pytest.raises(ValidationError):
        UserSettingsUpdateStrict(slurm_user="NEW_SLURM_USER")

    # Verify that a series of attributes can be made None
    nullable_attributes = [
        "ssh_host",
        "ssh_username",
        "ssh_private_key_path",
        "ssh_tasks_dir",
        "ssh_jobs_dir",
        "slurm_user",
    ]
    for key in nullable_attributes:
        update_request_body = UserSettingsUpdate(**{key: None})
        assert getattr(update_request_body, key) is None
        assert key in update_request_body.model_dump(exclude_unset=True)
        assert key not in update_request_body.model_dump(exclude_none=True)


def test_unit_val_absolute_path():
    val_absolute_path("/path")
    with pytest.raises(ValueError):
        val_absolute_path("   ")
    with pytest.raises(ValueError):
        val_absolute_path("non/absolute/path")
