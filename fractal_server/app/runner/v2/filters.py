from copy import copy
from typing import Optional

from .images import ImageAttribute
from .images import SingleImage
from .utils import ipjson
from .utils import pjson

FilterSet = dict[str, ImageAttribute]


def _filter_image_list(
    images: list[SingleImage],
    filters: Optional[FilterSet] = None,
) -> list[SingleImage]:

    if filters is None:
        # When no filter is provided, return all images
        return images

    filtered_images = []
    for this_image in images:
        include_this_image = True
        for key, value in filters.items():
            # If the FilterSet input includes the key-value pair
            # "attribute": None, then we ignore "attribute"
            if value is None:
                continue
            if this_image.get(key, None) != value:
                include_this_image = False
                break
        if include_this_image:
            filtered_images.append(copy(this_image))
    return filtered_images


def filter_images(
    *,
    dataset_images: list[SingleImage],
    dataset_filters: Optional[FilterSet] = None,
    wftask_filters: Optional[FilterSet] = None,
) -> list[SingleImage]:

    current_filters = copy(dataset_filters)
    current_filters.update(wftask_filters)
    print(f"[filter_images] Dataset filters:\n{ipjson(dataset_filters)}")
    print(f"[filter_images] WorkflowTask filters:\n{ipjson(wftask_filters)}")
    print(f"[filter_images] Dataset images:\n{ipjson(dataset_images)}")
    print(
        f"[filter_images] Current selection filters:\n{ipjson(current_filters)}"
    )
    filtered_images = _filter_image_list(
        dataset_images,
        filters=current_filters,
    )
    print(f"[filter_images] Filtered image list:  {pjson(filtered_images)}")
    return filtered_images
