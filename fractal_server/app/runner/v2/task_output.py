from typing import Optional

from pydantic import BaseModel

from ....images import SingleImage
from .models import DictStrAny


class TaskOutput(BaseModel):
    added_images: Optional[list[SingleImage]] = None
    edited_images: Optional[list[SingleImage]] = None
    removed_images: Optional[list[SingleImage]] = None
    new_attribute_filters: Optional[DictStrAny] = None
    new_flag_filters: Optional[dict[str, bool]] = None

    class Config:
        extra = "forbid"
