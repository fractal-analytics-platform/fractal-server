import pytest
from pydantic import ValidationError

from fractal_server.images import Filters
from fractal_server.images import SingleImage
from fractal_server.images.tools import filter_image_list
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import match_filter

N = 100
images = [
    SingleImage(
        zarr_url=f"/a/b/c{i}.zarr",
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


def test_find_image_by_zarr_url():

    for i in range(N):
        image_search = find_image_by_zarr_url(
            zarr_url=f"/a/b/c{i}.zarr", images=images
        )
        assert image_search["image"]["zarr_url"] == f"/a/b/c{i}.zarr"
        assert image_search["index"] == i

    image_search = find_image_by_zarr_url(zarr_url="/xxx", images=images)
    assert image_search is None


def test_match_filter():

    image = SingleImage(
        zarr_url="/a/b/c0.zarr",
        attributes=dict(
            name="a",
            num=0,
        ),
        types=dict(
            a=True,
            b=False,
        ),
    ).dict()

    f = Filters(
        attributes_include=dict(foo=["bar"])
    )  # not existing attribute # TODO

    f = Filters(attributes_include=dict(name=["a"]))
    assert match_filter(image, f) is True

    f = Filters(attributes_include=dict(num=[0]))
    assert match_filter(image, f) is True

    f = Filters(
        attributes_include=dict(
            name=["a"],
            num=[0],
        )
    )
    assert match_filter(image, f) is True

    f = Filters(
        attributes_include=dict(
            name=["a"],
            num=[0],
            foo=["bar"],  # not existing attribute
        )
    )
    assert match_filter(image, f) is False

    f = Filters(
        attributes_include=dict(
            name=["a"],
            num=["0"],  # int as string
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
        attributes_include=dict(name=["a"]),
        types=dict(a=True),
    )
    assert match_filter(image, f) is True
    f = Filters(
        attributes_include=dict(name=["a"]),
        types=dict(a=False),
    )
    assert match_filter(image, f) is False
    f = Filters(
        attributes_include=dict(name=["b"]),
        types=dict(a=True),
    )
    assert match_filter(image, f) is False
    f = Filters(
        attributes_include=dict(name=["a"]),
        types=dict(
            x=False,
            y=False,
            z=False,
        ),
    )
    assert match_filter(image, f) is True
    f = Filters(
        attributes_include=dict(name=["a"]),
        types=dict(
            x=False,
            y=False,
            z=True,
        ),
    )
    assert match_filter(image, f) is False

    image1 = SingleImage(
        zarr_url="/image", attributes=dict(a="value1", b="value2")
    ).dict()
    image2 = SingleImage(
        zarr_url="/image", attributes=dict(a="value3", b="value4")
    ).dict()

    # case 1
    assert match_filter(image1, Filters()) is True

    # case 2
    with pytest.raises(ValidationError):
        Filters(attributes_include=dict(a=[]))

    # case 3
    filter = Filters(attributes_include=dict(a=["value1"]))
    assert match_filter(image1, filter) is True
    assert match_filter(image2, filter) is False

    # case 4
    filter = Filters(attributes_include=dict(a=["value1", "value3"]))
    assert match_filter(image1, filter) is True
    assert match_filter(image2, filter) is True

    # case 5
    with pytest.raises(ValidationError):
        Filters(
            attributes_include=dict(a=["value1"]),
            attributes_exclude=dict(a=["value2"]),
        )

    # case 6
    filter = Filters(
        attributes_include=dict(a=["value1"]),
        attributes_exclude=dict(b=["value4"]),
    )
    assert match_filter(image1, filter) is True
    assert match_filter(image2, filter) is False


def test_filter_image_list():
    # Empty
    res = filter_image_list(images, Filters())
    assert res == images
    # Attributes
    f = Filters(attributes_include=dict(name=["a"]))
    res = filter_image_list(images, f)
    k = (N // 2) if not N % 2 else (N + 1) // 2
    assert len(res) == k
    f = Filters(attributes_include=dict(name=["b"]))
    res = filter_image_list(images, f)
    assert len(res) == N - k
    f = Filters(attributes_include=dict(num=[0]))
    res = filter_image_list(images, f)
    assert len(res) == len([i for i in range(N) if i % 3 == 0])
    f = Filters(attributes_include=dict(num=[1]))
    res = filter_image_list(images, f)
    assert len(res) == len([i for i in range(N) if i % 3 == 1])
    f = Filters(attributes_include=dict(num=[2]))
    res = filter_image_list(images, f)
    assert len(res) == len([i for i in range(N) if i % 3 == 2])
    f = Filters(attributes_include=dict(name=["foo"]))
    res = filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(attributes_include=dict(num=[3]))
    res = filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(attributes_include=dict(name=["a"], num=[3]))
    res = filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(attributes_include=dict(name=["foo"], num=[0]))
    res = filter_image_list(images, f)
    assert len(res) == 0
    f = Filters(
        types=dict(
            a=True,
            b=True,
        )
    )
    res = filter_image_list(images, f)
    assert len(res) == N // 2 - N // 3 + 1
