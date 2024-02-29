from typing import Optional

from pydantic import BaseModel

from ....images import SingleImage
from .models import DictStrAny


class TaskOutput(BaseModel):
    added_images: Optional[list[SingleImage]] = None
    """List of new images added by a given task instance."""

    edited_images: Optional[list[SingleImage]] = None
    """List of images edited by a given task instance."""

    removed_images: Optional[list[SingleImage]] = None

    new_filters: Optional[DictStrAny] = None
    """
    *Global* filters (common to all images) added by this task.

    Note: the right place for these filters would be in the task manifest,
    but this attribute is useful for the ones which determined at runtime
    (e.g. the plate name).
    """

    buffer: Optional[DictStrAny] = None
    """
    Metadata used for communication between an init task and its (parallel)
    companion task.
    """

    parallelization_list: Optional[list[DictStrAny]] = None
    """
    Used in the output of an init task, to expose customizable parallelization
    of the companion task.
    """
    # FIXME if parallelization_list is set maybe other attributes cannot be set

    class Config:
        extra = "forbid"


class ParallelTaskOutput(BaseModel):
    class Config:
        extra = "forbid"

    added_images: Optional[list[SingleImage]] = None
    edited_images: Optional[list[SingleImage]] = None
    removed_images: Optional[list[SingleImage]] = None
    new_filters: Optional[DictStrAny] = None  # FIXME
