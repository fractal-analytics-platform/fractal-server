from fractal_server.images import Filters
from fractal_server.images import SingleImage
from fractal_server.images.tools import _filter_image_list
from fractal_server.images.tools import find_image_by_path
from fractal_server.images.tools import match_filter

N = 100
images = [
    SingleImage(
        path=f"/a/b/c{i}.zarr",
        attributes=dict(
            name=("a" if i % 2 == 0 else "b"),
            num=i % 3,
        ),
        types=dict(
            a=(i <= N // 2),
            b=(i >= N // 3),
        ),
    ).dict()
    for i in range(N)
]


def test_find_image_by_path():

    for i in range(N):
        image_search = find_image_by_path(
            path=f"/a/b/c{i}.zarr", images=images
        )
        assert image_search["image"]["path"] == f"/a/b/c{i}.zarr"
        assert image_search["index"] == i

    image_search = find_image_by_path(path="/xxx", images=images)
    assert image_search is None


def test_match_filter():

    image = SingleImage(
        path="/a/b/c0.zarr",
        attributes=dict(
            name="a",
            num=0,
        ),
        types=dict(
            a=True,
            b=False,
        ),
    ).dict()

    # Empty
    assert match_filter(image, Filters()) is True

    # Attributes

    f = Filters(attributes=dict(foo="bar"))  # not existing attribute
    assert match_filter(image, f) is False

    f = Filters(attributes=dict(name="a"))
    assert match_filter(image, f) is True

    f = Filters(attributes=dict(num=0))
    assert match_filter(image, f) is True

    f = Filters(
        attributes=dict(
            name="a",
            num=0,
        )
    )
    assert match_filter(image, f) is True

    f = Filters(
        attributes=dict(
            name="a",
            num=0,
            foo="bar",  # not existing attribute
        )
    )
    assert match_filter(image, f) is False

    f = Filters(
        attributes=dict(
            name="a",
            num="0",  # int as string
        )
    )
    assert match_filter(image, f) is False

    # Types

    f = Filters(types=dict(a=True))
    assert match_filter(image, f) is True
    f = Filters(types=dict(b=False))
    assert match_filter(image, f) is True
    f = Filters(
        types=dict(
            a=True,
            b=False,
        )
    )
    assert match_filter(image, f) is True
    f = Filters(
        types=dict(
            a=False,
        )
    )
    assert match_filter(image, f) is False
    f = Filters(
        types=dict(
            a=True,
            b=True,
        )
    )
    assert match_filter(image, f) is False
    f = Filters(
        types=dict(
            c=True,  # not existing 'True' types are checked
        )
    )
    assert match_filter(image, f) is False
    f = Filters(
        types=dict(
            c=False,  # not existing 'False' types are ignored
        )
    )
    assert match_filter(image, f) is True
    f = Filters(
        types=dict(
            a=True,
            b=False,
            c=True,
        )
    )
    assert match_filter(image, f) is False
    f = Filters(
        types=dict(
            a=True,
            b=False,
            c=False,
        )
    )
    assert match_filter(image, f) is True

    # Both

    f = Filters(
        attributes=dict(
            name="a",
        ),
        types=dict(a=True),
    )
    assert match_filter(image, f) is True
    f = Filters(
        attributes=dict(
            name="a",
        ),
        types=dict(a=False),
    )
    assert match_filter(image, f) is False
    f = Filters(
        attributes=dict(
            name="b",
        ),
        types=dict(a=True),
    )
    assert match_filter(image, f) is False
    f = Filters(
        attributes=dict(
            name="a",
        ),
        types=dict(
            x=False,
            y=False,
            z=False,
        ),
    )
    assert match_filter(image, f) is True
    f = Filters(
        attributes=dict(
            name="a",
        ),
        types=dict(
            x=False,
            y=False,
            z=True,
        ),
    )
    assert match_filter(image, f) is False


def test_filter_image_list():
    # Empty
    res = _filter_image_list(images, Filters())
    assert res == images
    # Attributes
    f = Filters(attributes=dict(name="a"))
    res = _filter_image_list(images, f)
    k = (N // 2) if not N % 2 else (N + 1) // 2
    assert len(res) == k
    f = Filters(attributes=dict(name="b"))
    res = _filter_image_list(images, f)
    assert len(res) == N - k
    f = Filters(attributes=dict(num=0))
    res = _filter_image_list(images, f)
    assert len(res) == len([i for i in range(N) if i % 3 == 0])
    f = Filters(attributes=dict(num=1))
    res = _filter_image_list(images, f)
    assert len(res) == len([i for i in range(N) if i % 3 == 1])
    f = Filters(attributes=dict(num=2))
    res = _filter_image_list(images, f)
    assert len(res) == len([i for i in range(N) if i % 3 == 2])
    f = Filters(attributes=dict(name="foo"))
    res = _filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(attributes=dict(num=3))
    res = _filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(attributes=dict(name="a", num=3))
    res = _filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(attributes=dict(name="foo", num=0))
    res = _filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(
        types=dict(
            a=True,
            b=True,
        )
    )
    res = _filter_image_list(images, f)
    assert len(res) == N // 2 - N // 3 + 1
