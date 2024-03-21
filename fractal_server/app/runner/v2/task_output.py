from typing import Optional

from pydantic import BaseModel
from pydantic import Field

from ....images import SingleImage
from .models import DictStrAny


class TaskOutputFilters(BaseModel):
    attributes: DictStrAny = Field(default_factory=dict)
    flags: dict[str, bool] = Field(default_factory=dict)


class TaskOutput(BaseModel):
    image_list_updates: list[SingleImage] = Field(default_factory=list)
    image_list_removals: list[str] = Field(default_factory=list)
    filters: Optional[TaskOutputFilters] = None

    class Config:
        extra = "forbid"

    def check_paths_are_unique(self) -> None:
        paths = [img.path for img in self.image_list_updates]
        paths.extend(self.image_list_removals)
        if len(paths) != len(set(paths)):
            duplicates = [path for path in set(paths) if paths.count(path) > 1]
            msg = (
                "TaskOutput image-list updates/removals has non-unique paths:"
            )
            for duplicate in duplicates:
                msg = f"{msg}\n{duplicate}"
            raise ValueError(msg)
