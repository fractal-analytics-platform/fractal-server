from typing import TypeVar

from pydantic.main import ModelMetaclass

from ....images import SingleImage
from .task_interface import InitArgsModel

T = TypeVar("T", SingleImage, InitArgsModel)


def deduplicate_list(
    this_list: list[T], PydanticModel: ModelMetaclass
) -> list[T]:
    """
    Custom replacement for `set(this_list)`, when items are Pydantic-model
    instances and then non-hashable (e.g. SingleImage or InitArgsModel).
    """
    this_list_dict = [this_item.dict() for this_item in this_list]
    new_list_dict = []
    for this_dict in this_list_dict:
        if this_dict not in new_list_dict:
            new_list_dict.append(this_dict)
    new_list = [PydanticModel(**this_dict) for this_dict in new_list_dict]
    return new_list
