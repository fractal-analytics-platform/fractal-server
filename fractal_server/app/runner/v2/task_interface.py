from typing import Any

from pydantic import BaseModel
from pydantic import Field

from ....images import SingleImage
from fractal_server.images import Filters


class TaskOutput(BaseModel):
    class Config:
        extra = "forbid"

    image_list_updates: list[SingleImage] = Field(default_factory=list)
    image_list_removals: list[str] = Field(default_factory=list)
    filters: Filters = Field(default_factory=Filters)

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


class InitArgsModel(BaseModel):
    class Config:
        extra = "forbid"

    path: str
    init_args: dict[str, Any] = Field(default_factory=dict)


class InitTaskOutput(BaseModel):
    class Config:
        extra = "forbid"

    parallelization_list: list[InitArgsModel] = Field(default_factory=list)
