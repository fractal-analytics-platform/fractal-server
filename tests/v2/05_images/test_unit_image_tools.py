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

    # Empty
    assert match_filter(image) is True

    # Attributes

    # not existing attribute
    assert match_filter(image, attribute_filters=dict(foo="bar")) is False

    assert match_filter(image, attribute_filters=dict(name="a")) is True

    assert match_filter(image, attribute_filters=dict(num=0)) is True

    assert (
        match_filter(
            image,
            attribute_filters=dict(
                name="a",
                num=0,
            ),
        )
        is True
    )

    # not existing attribute
    assert (
        match_filter(image, attribute_filters=dict(name="a", num=0, foo="bar"))
        is False
    )

    # int as string
    assert (
        match_filter(image, attribute_filters=dict(name="a", num="0")) is False
    )

    # Types

    assert match_filter(image, type_filters=dict(a=True)) is True
    assert match_filter(image, type_filters=dict(b=False)) is True
    assert match_filter(image, type_filters=dict(a=True, b=False)) is True
    assert match_filter(image, type_filters=dict(a=False)) is False
    assert match_filter(image, type_filters=dict(a=True, b=True)) is False
    # not existing 'True' types are checked
    assert match_filter(image, type_filters=dict(c=True)) is False
    # not existing 'False' types are ignored
    assert match_filter(image, type_filters=dict(c=False)) is True
    assert (
        match_filter(image, type_filters=dict(a=True, b=False, c=True))
        is False
    )
    assert (
        match_filter(image, type_filters=dict(a=True, b=False, c=False))
        is True
    )

    # Both

    assert (
        match_filter(
            image, attribute_filters=dict(name="a"), type_filters=dict(a=True)
        )
        is True
    )
    assert (
        match_filter(
            image, attribute_filters=dict(name="a"), type_filters=dict(a=False)
        )
        is False
    )
    assert (
        match_filter(
            image, attribute_filters=dict(name="b"), type_filters=dict(a=True)
        )
        is False
    )
    assert (
        match_filter(
            image,
            attribute_filters=dict(name="a"),
            type_filters=dict(x=False, y=False, z=False),
        )
        is True
    )
    assert (
        match_filter(
            image,
            attribute_filters=dict(name="a"),
            type_filters=dict(x=False, y=False, z=True),
        )
        is False
    )


def test_attribute_filters_set_to_none():
    """
    This test shows how to disable a previously-set attribute filter by just
    using `dict.update` (that is, without e.g. `dict.pop`).
    """
    # Image not matching filter
    image = dict(
        types={},
        attributes={"key2": "invalid-value2"},
    )
    attribute_filters = {"key2": ["value2"]}
    assert not match_filter(
        image=image,
        type_filters={},
        attribute_filters=attribute_filters,
    )

    # Unset filter by replacing ["value2"] with None
    attribute_filters.update({"key2": None})

    # Image matches filter
    assert match_filter(
        image=image,
        type_filters={},
        attribute_filters=attribute_filters,
    )


def test_filter_image_list():
    # Empty
    res = filter_image_list(images)
    assert res == images
    # Attributes
    res = filter_image_list(images, attribute_filters=dict(name="a"))
    k = (N // 2) if not N % 2 else (N + 1) // 2
    assert len(res) == k
    res = filter_image_list(images, attribute_filters=dict(name="b"))
    assert len(res) == N - k
    res = filter_image_list(images, attribute_filters=dict(num=0))
    assert len(res) == len([i for i in range(N) if i % 3 == 0])
    res = filter_image_list(images, attribute_filters=dict(num=1))
    assert len(res) == len([i for i in range(N) if i % 3 == 1])
    res = filter_image_list(images, attribute_filters=dict(num=2))
    assert len(res) == len([i for i in range(N) if i % 3 == 2])
    res = filter_image_list(images, attribute_filters=dict(name="foo"))
    assert len(res) == 0
    res = filter_image_list(images, attribute_filters=dict(num=3))
    assert len(res) == 0
    res = filter_image_list(images, attribute_filters=dict(name="a", num=3))
    assert len(res) == 0
    res = filter_image_list(images, attribute_filters=dict(name="foo", num=0))
    assert len(res) == 0
    res = filter_image_list(images, type_filters=dict(a=True, b=True))
    assert len(res) == N // 2 - N // 3 + 1
