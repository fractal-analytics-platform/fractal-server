from copy import copy
from typing import Any
from typing import Optional
from typing import TypeVar
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


T = TypeVar("T")


def val_scalar_dict(attribute: str):
    def val(
        dict_str_any: dict[str, Any],
    ) -> dict[str, Union[int, float, str, bool, None]]:
        for key, value in dict_str_any.items():
            if not isinstance(value, (int, float, str, bool, type(None))):
                raise ValueError(
                    f"{attribute}[{key}] must be a scalar (int, float, str, "
                    f"bool, or None). Given {value} ({type(value)})"
                )
        return dict_str_any

    return val


class SingleImage(BaseModel):
    path: str
    attributes: dict[str, Any] = Field(default_factory=dict)

    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )

    def match_filter(self, filters: Optional[dict[str, Any]] = None):
        if filters is None:
            return True
        for key, value in filters.items():
            if value is None:
                continue
            if self.attributes.get(key) != value:
                return False
        return True


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


def deduplicate_list(this_list: list[T]) -> list[T]:
    """
    Custom replacement for `set(this_list)`, when items are of a non-hashable
    type T (e.g. dict or SingleImage).
    """
    new_list = []
    for this_item in this_list:
        if this_item not in new_list:
            new_list.append(this_item)
    return new_list


def _filter_image_list(
    images: list[SingleImage],
    filters: Optional[dict[str, Any]] = None,
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
    dataset_filters: Optional[dict[str, Any]] = None,
    wftask_filters: Optional[dict[str, Any]] = None,
) -> list[SingleImage]:

    current_filters = copy(dataset_filters)
    current_filters.update(wftask_filters)

    filtered_images = _filter_image_list(
        dataset_images,
        filters=current_filters,
    )
    return filtered_images
