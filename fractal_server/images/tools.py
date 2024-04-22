from copy import copy
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from fractal_server.images import Filters


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


def match_filter(image: dict[str, Any], filters: Filters) -> bool:
    """
    Find whether an image matches a filter set.

    Arguments:
        image: A single image.
        filters: A set of filters.

    Returns:
        Whether the image matches the filter set.
    """
    # Verify match with types (using a False default)
    for key, value in filters.types.items():
        if image["types"].get(key, False) != value:
            return False
    # Verify match with attributes (only for non-None filters)
    for key, value in filters.attributes.items():
        if value is None:
            continue
        if image["attributes"].get(key) != value:
            return False
    return True


def filter_image_list(
    images: list[dict[str, Any]],
    filters: Filters,
) -> list[dict[str, Any]]:
    """
    Compute a sublist with images that match a filter set.

    Arguments:
        images: A list of images.
        filters: A set of filters.

    Returns:
        List of the `images` elements which match the filter set.
    """

    # When no filter is provided, return all images
    if filters.attributes == {} and filters.types == {}:
        return images

    filtered_images = [
        copy(this_image)
        for this_image in images
        if match_filter(this_image, filters=filters)
    ]
    return filtered_images
