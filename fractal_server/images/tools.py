from copy import copy
from typing import Any
from typing import Literal
from typing import Union

from fractal_server.types import AttributeFilters

ImageSearch = dict[Literal["image", "index"], Union[int, dict[str, Any]]]


def find_image_by_zarr_url(
    *,
    images: list[dict[str, Any]],
    zarr_url: str,
) -> ImageSearch | None:
    """
    Return a copy of the image with a given zarr_url, and its positional index.

    Arguments:
        images: List of images.
        zarr_url: Path that the returned image must have.

    Returns:
        The first image from `images` which has zarr_url equal to `zarr_url`.
    """
    image_urls = [img["zarr_url"] for img in images]
    try:
        ind = image_urls.index(zarr_url)
    except ValueError:
        return None
    return dict(image=copy(images[ind]), index=ind)


def match_filter(
    *,
    image: dict[str, Any],
    type_filters: dict[str, bool],
    attribute_filters: AttributeFilters,
) -> bool:
    """
    Find whether an image matches a filter set.

    Arguments:
        image: A single image.
        type_filters:
        attribute_filters:

    Returns:
        Whether the image matches the filter set.
    """

    # Verify match with types (using a False default)
    for key, value in type_filters.items():
        if image["types"].get(key, False) != value:
            return False

    # Verify match with attributes (only for not-None filters)
    for key, values in attribute_filters.items():
        if image["attributes"].get(key) not in values:
            return False

    return True


def filter_image_list(
    images: list[dict[str, Any]],
    type_filters: dict[str, bool] | None = None,
    attribute_filters: AttributeFilters | None = None,
) -> list[dict[str, Any]]:
    """
    Compute a sublist with images that match a filter set.

    Arguments:
        images: A list of images.
        type_filters:
        attribute_filters:

    Returns:
        List of the `images` elements which match the filter set.
    """

    # When no filter is provided, return all images
    if type_filters is None and attribute_filters is None:
        return images
    actual_type_filters = type_filters or {}
    actual_attribute_filters = attribute_filters or {}

    filtered_images = [
        copy(this_image)
        for this_image in images
        if match_filter(
            image=this_image,
            type_filters=actual_type_filters,
            attribute_filters=actual_attribute_filters,
        )
    ]
    return filtered_images


def merge_type_filters(
    *,
    task_input_types: dict[str, bool],
    wftask_type_filters: dict[str, bool],
) -> dict[str, bool]:
    """
    Merge two type-filters sets, if they are compatible.
    """
    all_keys = set(task_input_types.keys()) | set(wftask_type_filters.keys())
    for key in all_keys:
        if (
            key in task_input_types.keys()
            and key in wftask_type_filters.keys()
            and task_input_types[key] != wftask_type_filters[key]
        ):
            raise ValueError(
                "Cannot merge type filters "
                f"`{task_input_types}` (from task) "
                f"and `{wftask_type_filters}` (from workflowtask)."
            )
    merged_dict = task_input_types
    merged_dict.update(wftask_type_filters)
    return merged_dict


def aggregate_attributes(images: list[dict[str, Any]]) -> dict[str, list[Any]]:
    """
    Given a list of images, this function returns a dictionary of all image
    attributes, each mapped to a sorted list of existing values.
    """
    attributes = {}
    for image in images:
        for k, v in image["attributes"].items():
            attributes.setdefault(k, []).append(v)
        for k, v in attributes.items():
            attributes[k] = list(set(v))
    sorted_attributes = {
        key: sorted(value) for key, value in attributes.items()
    }
    return sorted_attributes


def aggregate_types(images: list[dict[str, Any]]) -> list[str]:
    """
    Given a list of images, this function returns a list of all image types.
    """
    return list({type for image in images for type in image["types"].keys()})
