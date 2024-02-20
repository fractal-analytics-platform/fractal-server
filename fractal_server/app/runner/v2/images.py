from copy import copy
from typing import Optional

from .models import ImageAttributeType
from .models import SingleImage
from .utils import ipjson
from .utils import pjson


def find_image_by_path(
    *,
    images: list[SingleImage],
    path: str,
) -> SingleImage:
    """
    Return a copy of the image with a given path, from a list.

    Args:
        images: List of images.
        path: Path that the returned image must have.

    Returns:
        The first image from `images` which has path equal to `path`.
    """
    try:
        image = next(image for image in images if image.path == path)
        return copy(image)
    except StopIteration:
        raise ValueError(f"No image with {path=} found in image list.")


def _deduplicate_list_of_dicts(list_of_dicts: list[dict]) -> list[dict]:
    """
    Custom replacement for `set(list_of_dict)`, since `dict` is not hashable.
    """
    new_list_of_dicts = []
    for my_dict in list_of_dicts:
        if my_dict not in new_list_of_dicts:
            new_list_of_dicts.append(my_dict)
    return new_list_of_dicts


def _filter_image_list(
    images: list[SingleImage],
    filters: Optional[ImageAttributeType] = None,
) -> list[SingleImage]:

    if filters is None:
        # When no filter is provided, return all images
        return images

    filtered_images = []
    for this_image in images:
        if this_image.match_filter(filters):
            filtered_images.append(copy(this_image))
    return filtered_images


def filter_images(
    *,
    dataset_images: list[SingleImage],
    dataset_filters: Optional[ImageAttributeType] = None,
    wftask_filters: Optional[ImageAttributeType] = None,
) -> list[SingleImage]:

    current_filters = copy(dataset_filters)
    current_filters.update(wftask_filters)
    print(f"[filter_images] Dataset filters:\n{ipjson(dataset_filters)}")
    print(f"[filter_images] WorkflowTask filters:\n{ipjson(wftask_filters)}")
    print(f"[filter_images] Dataset images:\n{ipjson(dataset_images)}")
    print(
        "[filter_images] Current selection filters:\n"
        f"{ipjson(current_filters)}"
    )
    filtered_images = _filter_image_list(
        dataset_images,
        filters=current_filters,
    )
    print(f"[filter_images] Filtered image list:  {pjson(filtered_images)}")
    return filtered_images
