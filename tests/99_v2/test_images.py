import pytest
from images import find_image_by_path


def test_find_image_by_path():
    images = [dict(path="a", name="a"), dict(path="b")]

    image = find_image_by_path(path="a", images=images)
    assert image["name"] == "a"

    with pytest.raises(ValueError):
        find_image_by_path(path="invalid", images=images)
