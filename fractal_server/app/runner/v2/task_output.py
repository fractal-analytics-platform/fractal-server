from pydantic import BaseModel
from pydantic import Field

from ....images import SingleImage
from .models import DictStrAny


class TaskOutput(BaseModel):

    # CRUD images
    added_images: list[SingleImage] = Field(default_factory=list)
    edited_images: list[SingleImage] = Field(default_factory=list)
    removed_images: list[SingleImage] = Field(default_factory=list)

    # New filters
    new_attribute_filters: DictStrAny = Field(default_factory=dict)
    new_flag_filters: dict[str, bool] = Field(default_factory=dict)

    class Config:
        extra = "forbid"
