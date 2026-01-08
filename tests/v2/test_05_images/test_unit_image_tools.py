import pytest

from fractal_server.images.tools import filter_image_list
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import match_filter
from fractal_server.images.tools import merge_type_filters


def test_find_image_by_zarr_url():
    images = [{"zarr_url": "/x"}, {"zarr_url": "/y"}, {"zarr_url": "/z"}]
    res = find_image_by_zarr_url(zarr_url="/x", images=images)
    assert res == {
        "index": 0,
        "image": {"zarr_url": "/x"},
    }
    res = find_image_by_zarr_url(zarr_url="/y", images=images)
    assert res == {
        "index": 1,
        "image": {"zarr_url": "/y"},
    }
    res = find_image_by_zarr_url(zarr_url="/z", images=images)
    assert res == {
        "index": 2,
        "image": {"zarr_url": "/z"},
    }
    res = find_image_by_zarr_url(zarr_url="/k", images=images)
    assert res is None


def test_match_filter():
    # empty filters (always match)
    assert match_filter(image=..., type_filters={}, attribute_filters={})

    image = {"types": {"a": True, "b": False}, "attributes": {"a": 1, "b": 2}}

    # type filters
    # a
    assert match_filter(
        image=image, type_filters={"a": True}, attribute_filters={}
    )
    assert not match_filter(
        image=image, type_filters={"a": False}, attribute_filters={}
    )
    # b
    assert not match_filter(
        image=image, type_filters={"b": True}, attribute_filters={}
    )
    assert match_filter(
        image=image, type_filters={"b": False}, attribute_filters={}
    )
    # c
    assert not match_filter(
        image=image, type_filters={"c": True}, attribute_filters={}
    )
    assert match_filter(
        image=image, type_filters={"c": False}, attribute_filters={}
    )
    # a b c
    assert match_filter(
        image=image,
        type_filters={"a": True, "b": False, "c": False},
        attribute_filters={},
    )
    assert not match_filter(
        image=image,
        type_filters={"a": False, "b": False, "c": False},
        attribute_filters={},
    )
    assert not match_filter(
        image=image,
        type_filters={"a": True, "b": True, "c": False},
        attribute_filters={},
    )
    assert not match_filter(
        image=image,
        type_filters={"a": False, "b": True, "c": False},
        attribute_filters={},
    )

    # attribute filters
    assert match_filter(
        image=image, type_filters={}, attribute_filters={"a": [1]}
    )
    assert match_filter(
        image=image, type_filters={}, attribute_filters={"a": [1], "b": [1, 2]}
    )
    assert not match_filter(
        image=image, type_filters={}, attribute_filters={"a": [0], "b": [1, 2]}
    )

    # both
    assert match_filter(
        image=image, type_filters={"a": True}, attribute_filters={"a": [1]}
    )
    assert not match_filter(
        image=image, type_filters={"a": False}, attribute_filters={"a": [1]}
    )
    assert not match_filter(
        image=image, type_filters={"a": True}, attribute_filters={"a": [0]}
    )


def test_filter_image_list():
    images = [
        {"types": {"a": True}, "attributes": {"a": 1, "b": 2}},
        {"types": {"a": True}, "attributes": {"a": 2, "b": 2}},
        {"types": {"a": False}, "attributes": {"a": 1, "b": 1}},
        {"types": {}, "attributes": {"a": 1, "b": 1}},
        {"types": {}, "attributes": {}},
    ]

    # empty
    res = filter_image_list(images)
    assert res == images
    res = filter_image_list(images, type_filters={})
    assert res == images
    res = filter_image_list(images, attribute_filters={})
    assert res == images
    res = filter_image_list(images, type_filters={}, attribute_filters={})
    assert res == images

    # type filters
    res = filter_image_list(images, type_filters={"a": True})
    assert len(res) == 2
    res = filter_image_list(images, type_filters={"a": False})
    assert len(res) == 3  # complementary of 2
    res = filter_image_list(images, type_filters={"b": True})
    assert len(res) == 0
    res = filter_image_list(images, type_filters={"b": False})
    assert len(res) == 5

    # attribute filters
    res = filter_image_list(images, attribute_filters={"a": [1]})
    assert len(res) == 3
    res = filter_image_list(images, attribute_filters={"a": [1, 2]})
    assert len(res) == 4

    # both
    res = filter_image_list(
        images, type_filters={"a": True}, attribute_filters={"a": [1]}
    )
    assert len(res) == 1
    res = filter_image_list(
        images, type_filters={"a": True}, attribute_filters={"a": [1, 2]}
    )
    assert len(res) == 2
    res = filter_image_list(
        images,
        type_filters={"a": True},
        attribute_filters={"a": [1, 2], "b": [-1]},
    )
    assert len(res) == 0


def test_merge_type_filters():
    task_input_types = dict(key1=False, key2=True)
    wftask_type_filters = dict(key1=False, key3=True)
    merged = merge_type_filters(
        task_input_types=task_input_types,
        wftask_type_filters=wftask_type_filters,
    )
    assert merged == dict(key1=False, key2=True, key3=True)

    task_input_types = dict()
    wftask_type_filters = dict(key1=False, key3=True)
    merged = merge_type_filters(
        task_input_types=task_input_types,
        wftask_type_filters=wftask_type_filters,
    )
    assert merged == wftask_type_filters

    task_input_types = dict(key1=False, key2=True)
    wftask_type_filters = dict()
    merged = merge_type_filters(
        task_input_types=task_input_types,
        wftask_type_filters=wftask_type_filters,
    )
    assert merged == task_input_types

    task_input_types = dict(key1=False, key2=True)
    wftask_type_filters = dict(key1=True)
    with pytest.raises(ValueError, match="Cannot merge"):
        merge_type_filters(
            task_input_types=task_input_types,
            wftask_type_filters=wftask_type_filters,
        )


def test_merge_type_filters_mutable_args():
    """
    Test for"""
    task_input_types = dict(key1=False, key2=True)
    wftask_type_filters = dict(key1=False, key3=True)
    merge_type_filters(
        task_input_types=task_input_types,
        wftask_type_filters=wftask_type_filters,
    )
    assert task_input_types == dict(key1=False, key2=True)
    assert wftask_type_filters == dict(key1=False, key3=True)
