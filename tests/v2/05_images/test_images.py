import pytest

from fractal_server.images import find_image_by_path
from fractal_server.images import SingleImage


def test_find_image_by_path():
    images = [
        SingleImage(path="a", attributes=dict(name="a")),
        SingleImage(path="b"),
    ]

    image = find_image_by_path(path="a", images=images)
    assert image.attributes["name"] == "a"

    with pytest.raises(ValueError):
        find_image_by_path(path="invalid", images=images)
