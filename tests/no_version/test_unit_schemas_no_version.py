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
    # Valid S3 URLs
    val_s3_url("s3://my-bucket/path/to/file.txt")
    val_s3_url("s3://abc/key")
    val_s3_url("s3://my.bucket.name/some/deep/path/file")
    val_s3_url("s3://bucket-123/file")
    val_s3_url("s3://a1b/key")

    # Invalid pattern - not matching s3://bucket/key
    with pytest.raises(ValueError, match="must match pattern"):
        val_s3_url("http://bucket/key")
    with pytest.raises(ValueError, match="must match pattern"):
        val_s3_url("s3://bucket")
    with pytest.raises(ValueError, match="must match pattern"):
        val_s3_url("s3://bucket/")
    with pytest.raises(ValueError, match="must match pattern"):
        val_s3_url("bucket/key")

    # Bucket name length validation
    with pytest.raises(ValueError, match="between 3 and 63 characters"):
        val_s3_url("s3://ab/key")
    with pytest.raises(ValueError, match="between 3 and 63 characters"):
        val_s3_url(f"s3://{'a' * 64}/key")

    # Bucket name character validation
    with pytest.raises(ValueError, match="start and end with lowercase"):
        val_s3_url("s3://Bucket/key")
    with pytest.raises(ValueError, match="start and end with lowercase"):
        val_s3_url("s3://-bucket/key")
    with pytest.raises(ValueError, match="start and end with lowercase"):
        val_s3_url("s3://bucket-/key")
    with pytest.raises(ValueError, match="start and end with lowercase"):
        val_s3_url("s3://.bucket/key")

    # Adjacent periods
    with pytest.raises(ValueError, match="two adjacent periods"):
        val_s3_url("s3://my..bucket/key")

    # IP address format
    with pytest.raises(ValueError, match="not be formatted as an IP address"):
        val_s3_url("s3://192.168.1.1/key")

    # Prohibited prefixes
    with pytest.raises(ValueError, match="must not start with"):
        val_s3_url("s3://xn--bucket/key")
    with pytest.raises(ValueError, match="must not start with"):
        val_s3_url("s3://sthree-bucket/key")
    with pytest.raises(ValueError, match="must not start with"):
        val_s3_url("s3://amzn-s3-demo-bucket/key")

    # Prohibited suffixes
    with pytest.raises(ValueError, match="must not end with"):
        val_s3_url("s3://bucket-s3alias/key")
    with pytest.raises(ValueError, match="must not end with"):
        val_s3_url("s3://bucket--ol-s3/key")
    with pytest.raises(ValueError, match="must not end with"):
        val_s3_url("s3://bucket.mrap/key")
    with pytest.raises(ValueError, match="must not end with"):
        val_s3_url("s3://bucket--x-s3/key")
    with pytest.raises(ValueError, match="must not end with"):
        val_s3_url("s3://bucket--table-s3/key")

    # Key validation tests
    # Valid key - exactly 1024 bytes
    valid_key_1024 = "a" * 1024
    val_s3_url(f"s3://bucket/{valid_key_1024}")

    # Invalid key - exceeds 1024 bytes
    with pytest.raises(ValueError, match="must not exceed 1024 bytes"):
        invalid_key = "a" * 1025
        val_s3_url(f"s3://bucket/{invalid_key}")

    # Invalid key - UTF-8 characters causing byte length to exceed 1024
    # Each "文" character is 3 bytes in UTF-8
    # 342 characters × 3 bytes = 1026 bytes (exceeds 1024)
    with pytest.raises(ValueError, match="must not exceed 1024 bytes"):
        utf8_key = "文" * 342
        val_s3_url(f"s3://bucket/{utf8_key}")

    # Valid key with UTF-8 characters
    val_s3_url("s3://bucket/path/文件.txt")
    val_s3_url("s3://bucket/földer/file.txt")

    # Invalid key - non-UTF-8 characters
    with pytest.raises(
        ValueError,
        match="^S3 key must contain only valid UTF-8 characters",
    ):
        # Create a string with surrogate characters (invalid in UTF-8)
        invalid_utf8_key = "path/\udcff\udcfe/file"
        val_s3_url(f"s3://bucket/{invalid_utf8_key}")
