from copy import copy
from typing import Any
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator


def val_scalar_dict(attribute: str):
    def val(
        dict_str_any: dict[str, Any],
    ) -> dict[str, Union[int, float, str, bool]]:
        for key, value in dict_str_any.items():
            if not isinstance(value, (int, float, str, bool)):
                raise ValueError(
                    f"{attribute}[{key}] must be a scalar (int, float, str, "
                    f"bool, or None). Given {value} ({type(value)})"
                )
        return dict_str_any

    return val


class SingleImage(BaseModel):

    path: str
    origin: Optional[str] = None

    attributes: dict[str, Any] = Field(default_factory=dict)
    types: dict[str, bool] = Field(default_factory=dict)

    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )

    def match_filter(
        self,
        attribute_filters: Optional[dict[str, Any]] = None,
        type_filters: Optional[dict[str, bool]] = None,
    ):

        if type_filters is not None:
            for key, value in type_filters.items():
                if self.types.get(key, False) != value:
                    return False

        if attribute_filters is not None:
            for key, value in attribute_filters.items():
                if value is None:
                    continue
                if self.attributes.get(key) != value:
                    return False

        return True


def find_image_by_path(
    *,
    images: list[SingleImage],
    path: str,
) -> Optional[dict[str, Union[int, SingleImage]]]:
    """
    Return a copy of the image with a given path and its positional index.

    Args:
        images: List of images.
        path: Path that the returned image must have.

    Returns:
        The first image from `images` which has path equal to `path`.
    """
    image_paths = [img.path for img in images]
    try:
        ind = image_paths.index(path)
    except ValueError:
        return None
    return dict(image=copy(images[ind]), index=ind)


def _filter_image_list(
    images: list[SingleImage],
    attribute_filters: Optional[dict[str, Any]] = None,
    type_filters: Optional[dict[str, Any]] = None,
) -> list[SingleImage]:
    if attribute_filters is None and type_filters is None:
        # When no filter is provided, return all images
        return images

    filtered_images = []
    for this_image in images:
        if this_image.match_filter(
            attribute_filters=attribute_filters, type_filters=type_filters
        ):
            filtered_images.append(copy(this_image))
    return filtered_images
