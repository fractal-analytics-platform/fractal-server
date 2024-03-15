from typing import Optional

from pydantic import BaseModel
from pydantic import Field

from ....images import SingleImage
from .models import DictStrAny


class TaskOutput(BaseModel):
    added_images: Optional[list[SingleImage]] = None
    edited_images: Optional[list[SingleImage]] = None
    removed_images: Optional[list[SingleImage]] = None
    new_attribute_filters: Optional[DictStrAny] = None

    class Config:
        extra = "forbid"


class InitArgsModel(BaseModel):
    class Config:
        extra = "forbid"

    path: str
    init_args: DictStrAny = Field(default_factory=dict)


class InitTaskOutput(BaseModel):
    parallelization_list: list[InitArgsModel] = Field(default_factory=list)

    class Config:
        extra = "forbid"
