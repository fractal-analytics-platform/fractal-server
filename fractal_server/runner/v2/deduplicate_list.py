from typing import TypeVar

from .task_interface import InitArgsModel
from fractal_server.images import SingleImage
from fractal_server.images import SingleImageTaskOutput

T = TypeVar("T", SingleImage, SingleImageTaskOutput, InitArgsModel)


def deduplicate_list(
    this_list: list[T],
) -> list[T]:
    """
    Custom replacement for `set(this_list)`, when items are non-hashable.
    """
    new_list_dict = []
    new_list_objs = []
    for this_obj in this_list:
        this_dict = this_obj.model_dump()
        if this_dict not in new_list_dict:
            new_list_dict.append(this_dict)
            new_list_objs.append(this_obj)
    return new_list_objs
