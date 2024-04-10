from typing import TypeVar

from ....images import SingleImage
from ....images import SingleImageTaskOutput
from .task_interface import InitArgsModel

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
        this_dict = this_obj.dict()
        if this_dict not in new_list_dict:
            new_list_dict.append(this_dict)
            new_list_objs.append(this_obj)
    return new_list_objs
