from pydantic import BaseModel
from pydantic import Field

from ....images import SingleImage
from fractal_server.app.runner.v2.models import Filters


class TaskOutput(BaseModel):
    image_list_updates: list[SingleImage] = Field(default_factory=list)
    image_list_removals: list[str] = Field(default_factory=list)
    filters: Filters = Field(default_factory=Filters)

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
