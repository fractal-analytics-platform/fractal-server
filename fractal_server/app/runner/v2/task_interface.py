from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

from ....images import SingleImageTaskOutput
from fractal_server.urls import normalize_url


class TaskOutput(BaseModel):

    model_config = ConfigDict(extra="forbid")

    image_list_updates: list[SingleImageTaskOutput] = Field(
        default_factory=list
    )
    image_list_removals: list[str] = Field(default_factory=list)

    def check_zarr_urls_are_unique(self) -> None:
        zarr_urls = [img.zarr_url for img in self.image_list_updates]
        zarr_urls.extend(self.image_list_removals)
        if len(zarr_urls) != len(set(zarr_urls)):
            duplicates = [
                zarr_url
                for zarr_url in set(zarr_urls)
                if zarr_urls.count(zarr_url) > 1
            ]
            msg = (
                "TaskOutput "
                f"({len(self.image_list_updates)} image_list_updates and "
                f"{len(self.image_list_removals)} image_list_removals) "
                "has non-unique zarr_urls:"
            )
            for duplicate in duplicates:
                msg = f"{msg}\n{duplicate}"
            raise ValueError(msg)

    @field_validator("image_list_removals")
    @classmethod
    def normalize_paths(cls, v: list[str]) -> list[str]:
        return [normalize_url(zarr_url) for zarr_url in v]


class InitArgsModel(BaseModel):

    model_config = ConfigDict(extra="forbid")

    zarr_url: str
    init_args: dict[str, Any] = Field(default_factory=dict)

    @field_validator("zarr_url")
    @classmethod
    def normalize_path(cls, v: str) -> str:
        return normalize_url(v)


class InitTaskOutput(BaseModel):

    model_config = ConfigDict(extra="forbid")

    parallelization_list: list[InitArgsModel] = Field(default_factory=list)
