from fractal_server.images import SingleImage
from fractal_server.images.tools import find_image_by_path


def test_find_image_by_path():
    images = [
        SingleImage(path="a", attributes=dict(name="a")),
        SingleImage(path="b", types=dict(has_z=True)),
    ]

    image_search = find_image_by_path(path="a", images=images)
    assert image_search["image"].attributes["name"] == "a"
    assert image_search["index"] == 0

    image_search = find_image_by_path(path="b", images=images)
    assert image_search["image"].types["has_z"] is True
    assert image_search["index"] == 1

    image_search = find_image_by_path(path="invalid", images=images)
    assert image_search is None
