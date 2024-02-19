# Example image
# image = {"path": "/tmp/asasd", "dimensions": 3}
# Example filters
# filters = {"dimensions": 2, "illumination_corrected": False}
from copy import copy
from typing import Union


ImageAttribute = Union[str, bool, int, None]  # a scalar JSON object
SingleImage = dict[str, ImageAttribute]


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
        image = next(image for image in images if image["path"] == path)
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
