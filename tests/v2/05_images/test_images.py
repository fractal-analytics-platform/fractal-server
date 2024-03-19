from fractal_server.images import find_image_by_path
from fractal_server.images import SingleImage


def test_find_image_by_path():
    images = [
        SingleImage(path="a", attributes=dict(name="a")),
        SingleImage(path="b", flags=dict(has_z=True)),
    ]

    image = find_image_by_path(path="a", images=images)
    assert image.attributes["name"] == "a"

    image = find_image_by_path(path="invalid", images=images)
    assert image is None
