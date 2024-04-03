from copy import copy
from typing import Any
from typing import Optional
from typing import Union

from fractal_server.images import Filters
from fractal_server.images import SingleImage


def find_image_by_path(
    *,
    images: list[dict[str, Any]],
    path: str,
) -> Optional[dict[str, Union[int, dict[str, Any]]]]:
    """
    Return a copy of the image with a given path and its positional index.

    Args:
        images: List of images.
        path: Path that the returned image must have.

    Returns:
        The first image from `images` which has path equal to `path`.
    """
    image_paths = [img["path"] for img in images]
    try:
        ind = image_paths.index(path)
    except ValueError:
        return None
    return dict(image=copy(images[ind]), index=ind)


def match_filter(image: dict[str, Any], filters: Filters):
    for key, value in filters.types.items():
        if image["types"].get(key, False) != value:
            return False
    for key, value in filters.attributes.items():
        if value is None:
            continue
        if image["attributes"].get(key) != value:
            return False
    return True


def _filter_image_list(
    images: list[dict[str, Any]],
    filters: Filters,
) -> list[dict[str, Any]]:

    # When no filter is provided, return all images
    if filters.attributes == {} and filters.types == {}:
        return images

    filtered_images = []
    for this_image in images:
        if match_filter(this_image, filters=filters):
            filtered_images.append(copy(this_image))
    return filtered_images


def match_filter_SingleImage(image: SingleImage, filters: Filters):
    for key, value in filters.types.items():
        if image.types.get(key, False) != value:
            return False
    for key, value in filters.attributes.items():
        if value is None:
            continue
        if image.attributes.get(key) != value:
            return False
    return True


def _filter_image_list_SingleImage(
    images: list[SingleImage],
    filters: Filters,
) -> list[SingleImage]:

    # When no filter is provided, return all images
    if filters.attributes == {} and filters.types == {}:
        return images

    filtered_images = []
    for this_image in images:
        if match_filter_SingleImage(this_image, filters=filters):
            filtered_images.append(copy(this_image))
    return filtered_images
