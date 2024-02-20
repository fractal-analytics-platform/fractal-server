from typing import Optional

from pydantic import BaseModel

from .images import find_image_by_path
from .images import SingleImage
from .models import DictStrAny


class TaskOutput(BaseModel):
    new_images: Optional[list[SingleImage]] = None
    """List of new images added by a given task instance."""

    edited_images: Optional[list[SingleImage]] = None
    """List of images edited by a given task instance."""

    new_filters: Optional[
        DictStrAny
    ] = None  # FIXME: this does not actually work in Pydantic
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

    removed_images: Optional[list[SingleImage]] = None

    class Config:
        extra = "forbid"


class ParallelTaskOutput(BaseModel):
    class Config:
        extra = "forbid"

    new_images: Optional[list[SingleImage]] = None
    edited_images: Optional[list[SingleImage]] = None
    new_filters: Optional[DictStrAny] = None  # FIXME


def merge_outputs(
    task_outputs: list[ParallelTaskOutput],
    new_old_image_mapping: dict[str, str],
    old_dataset_images: list[SingleImage],
) -> TaskOutput:

    final_new_images = []
    final_edited_images = []
    final_removed_images = []
    final_new_filters = None

    for task_output in task_outputs:

        if task_output.new_images:
            for new_image in task_output.new_images:
                old_image = find_image_by_path(
                    images=old_dataset_images,
                    path=new_old_image_mapping[new_image.path],
                )
                # Propagate old-image attributes to new-image
                new_image.attributes = (
                    old_image.attributes | new_image.attributes
                )
                final_new_images.append(new_image)

        if task_output.edited_images:
            for edited_image in task_output.edited_images:
                final_edited_images.append(edited_image)

        if task_output.removed_images:
            for removed_image in task_output.removed_images:
                final_removed_images.append(removed_image)

        new_filters = task_output.new_filters
        if new_filters:
            if final_new_filters is None:
                final_new_filters = new_filters
            else:
                if final_new_filters != new_filters:
                    raise ValueError(
                        f"{new_filters=} but {final_new_filters=}"
                    )

    final_output = TaskOutput()
    if final_new_images:
        final_output.new_images = final_new_images
    if final_edited_images:
        final_output.edited_images = final_edited_images
    if final_new_filters:
        final_output.new_filters = final_new_filters
    if final_edited_images:
        final_output.removed_images = final_removed_images

    return final_output
