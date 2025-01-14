from copy import copy
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from fractal_server.images.models import AttributeFiltersType

ImageSearch = dict[Literal["image", "index"], Union[int, dict[str, Any]]]


def find_image_by_zarr_url(
    *,
    images: list[dict[str, Any]],
    zarr_url: str,
) -> Optional[ImageSearch]:
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
    attribute_filters: AttributeFiltersType,
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
        if values is None:
            continue
        if image["attributes"].get(key) not in values:
            return False

    return True


def filter_image_list(
    images: list[dict[str, Any]],
    type_filters: Optional[dict[str, bool]] = None,
    attribute_filters: Optional[AttributeFiltersType] = None,
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
