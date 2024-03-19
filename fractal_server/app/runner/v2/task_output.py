from pydantic import BaseModel
from pydantic import Field

from ....images import SingleImage
from .models import DictStrAny


class TaskOutput(BaseModel):

    # "CRUD" images
    added_images: list[SingleImage] = Field(default_factory=list)
    edited_image_paths: list[str] = Field(
        default_factory=list
    )  # TODO rename??
    removed_image_paths: list[str] = Field(default_factory=list)

    # New flags/attributes
    attributes: DictStrAny = Field(default_factory=dict)
    flags: dict[str, bool] = Field(default_factory=dict)

    class Config:
        extra = "forbid"


# TODO: validator that deduplicates lists of images? MAYBE
