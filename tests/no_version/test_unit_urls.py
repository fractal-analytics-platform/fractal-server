import pytest

from fractal_server.urls import normalize_url
from fractal_server.urls import url_is_relative_to
from fractal_server.urls import url_join


def test_url_join_local():
    assert url_join("/a/b", "c") == "/a/b/c"
    assert url_join("/a/b", "c", "d") == "/a/b/c/d"
    assert url_join("/a/b/", "c") == "/a/b/c"
    assert url_join("/a//b", "c") == "/a/b/c"


def test_url_join_s3():
    assert url_join("s3://bucket/key", "sub") == "s3://bucket/key/sub"
    assert url_join("s3://bucket/key", "sub", "file") == (
        "s3://bucket/key/sub/file"
    )
    assert url_join("s3://bucket", "key") == "s3://bucket/key"
    assert url_join("s3://bucket/a/b", "c") == "s3://bucket/a/b/c"


def test_url_is_relative_to_local():
    assert url_is_relative_to("/a/b/c", "/a/b") is True
    assert url_is_relative_to("/a/b", "/a/b") is True
    assert url_is_relative_to("/a/b", "/a") is True
    assert url_is_relative_to("/a/bc", "/a/b") is False
    assert url_is_relative_to("/x/y", "/a/b") is False
    assert url_is_relative_to("/a", "/a/b") is False


def test_url_is_relative_to_s3():
    assert url_is_relative_to("s3://bucket/key/sub", "s3://bucket/key") is True
    assert url_is_relative_to("s3://bucket/key", "s3://bucket/key") is True
    assert url_is_relative_to("s3://bucket/key2", "s3://bucket/key") is False
    assert url_is_relative_to("s3://other/key", "s3://bucket/key") is False
    assert url_is_relative_to("s3://bucket", "s3://bucket/key") is False


def test_url_is_relative_to_mixed():
    """Local and S3 URLs should never be relative to each other."""
    assert url_is_relative_to("/a/b", "s3://a/b") is False
    assert url_is_relative_to("s3://a/b", "/a/b") is False


def test_normalize_url_invalid():
    with pytest.raises(ValueError, match="URLs must begin"):
        normalize_url("http://example.com")
    with pytest.raises(ValueError, match="URLs must begin"):
        normalize_url("relative/path")
