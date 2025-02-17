from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import HttpUrl
from pydantic import model_validator

from fractal_server.app.schemas._validators import val_unique_list
from fractal_server.app.schemas._validators import valdict_keys
from fractal_server.app.schemas._validators import valstr
from fractal_server.string_tools import validate_cmd


class TaskCreateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None

    meta_non_parallel: Optional[dict[str, Any]] = None
    meta_parallel: Optional[dict[str, Any]] = None
    version: Optional[str] = None
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[str] = None

    input_types: dict[str, bool] = Field(default={})
    output_types: dict[str, bool] = Field(default={})

    category: Optional[str] = None
    modality: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    authors: Optional[str] = None

    # Validators
    @model_validator(mode="after")
    def validate_commands(self):
        command_parallel = self.command_parallel
        command_non_parallel = self.command_non_parallel
        if (command_parallel is None) and (command_non_parallel is None):
            raise ValueError(
                "Task must have at least one valid command "
                "(parallel and/or non_parallel)"
            )
        if command_parallel is not None:
            validate_cmd(command_parallel)
        if command_non_parallel is not None:
            validate_cmd(command_non_parallel)

        return self

    _name = field_validator("name")(classmethod(valstr("name")))
    _command_non_parallel = field_validator("command_non_parallel")(
        classmethod(valstr("command_non_parallel"))
    )
    _command_parallel = field_validator("command_parallel")(
        classmethod(valstr("command_parallel"))
    )
    _version = field_validator("version")(classmethod(valstr("version")))

    _meta_non_parallel = field_validator("meta_non_parallel")(
        classmethod(valdict_keys("meta_non_parallel"))
    )
    _meta_parallel = field_validator("meta_parallel")(
        classmethod(valdict_keys("meta_parallel"))
    )
    _args_schema_non_parallel = field_validator("args_schema_non_parallel")(
        classmethod(valdict_keys("args_schema_non_parallel"))
    )
    _args_schema_parallel = field_validator("args_schema_parallel")(
        classmethod(valdict_keys("args_schema_parallel"))
    )
    _args_schema_version = field_validator("args_schema_version")(
        classmethod(valstr("args_schema_version"))
    )
    _input_types = field_validator("input_types")(
        classmethod(valdict_keys("input_types"))
    )
    _output_types = field_validator("output_types")(
        classmethod(valdict_keys("output_types"))
    )

    _category = field_validator("category")(
        classmethod(valstr("category", accept_none=True))
    )
    _modality = field_validator("modality")(
        classmethod(valstr("modality", accept_none=True))
    )
    _authors = field_validator("authors")(
        classmethod(valstr("authors", accept_none=True))
    )

    @field_validator("tags")
    @classmethod
    def validate_list_of_strings(cls, value):
        for i, tag in enumerate(value):
            value[i] = valstr(f"tags[{i}]")(cls, tag)
        return val_unique_list("tags")(cls, value)

    @field_validator("docs_link", mode="after")
    @classmethod
    def validate_docs_link(cls, value):
        if value is not None:
            HttpUrl(value)
        return value


class TaskReadV2(BaseModel):
    id: int
    name: str
    type: Literal["parallel", "non_parallel", "compound"]
    source: Optional[str] = None
    version: Optional[str] = None

    command_non_parallel: Optional[str] = None
    command_parallel: Optional[str] = None
    meta_parallel: dict[str, Any]
    meta_non_parallel: dict[str, Any]
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    args_schema_version: Optional[str] = None
    docs_info: Optional[str] = None
    docs_link: Optional[str] = None
    input_types: dict[str, bool]
    output_types: dict[str, bool]

    taskgroupv2_id: Optional[int] = None

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: list[str]


class TaskUpdateV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_parallel: Optional[str] = None
    command_non_parallel: Optional[str] = None
    input_types: Optional[dict[str, bool]] = None
    output_types: Optional[dict[str, bool]] = None

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: Optional[list[str]] = None

    # Validators
    @field_validator("input_types", "output_types")
    @classmethod
    def val_is_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError
        return v

    _command_parallel = field_validator("command_parallel")(
        classmethod(valstr("command_parallel"))
    )
    _command_non_parallel = field_validator("command_non_parallel")(
        classmethod(valstr("command_non_parallel"))
    )
    _input_types = field_validator("input_types")(
        classmethod(valdict_keys("input_types"))
    )
    _output_types = field_validator("output_types")(
        classmethod(valdict_keys("output_types"))
    )

    _category = field_validator("category")(
        classmethod(valstr("category", accept_none=True))
    )
    _modality = field_validator("modality")(
        classmethod(valstr("modality", accept_none=True))
    )
    _authors = field_validator("authors")(
        classmethod(valstr("authors", accept_none=True))
    )

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, value):
        for i, tag in enumerate(value):
            value[i] = valstr(f"tags[{i}]")(cls, tag)
        return val_unique_list("tags")(cls, value)


class TaskImportV2(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pkg_name: str
    version: Optional[str] = None
    name: str
    _pkg_name = field_validator("pkg_name")(classmethod(valstr("pkg_name")))
    _version = field_validator("version")(
        classmethod(valstr("version", accept_none=True))
    )
    _name = field_validator("name")(classmethod(valstr("name")))


class TaskImportV2Legacy(BaseModel):
    source: str
    _source = field_validator("source")(classmethod(valstr("source")))


class TaskExportV2(BaseModel):
    pkg_name: str
    version: Optional[str] = None
    name: str

    _pkg_name = field_validator("pkg_name")(classmethod(valstr("pkg_name")))
    _version = field_validator("version")(
        classmethod(valstr("version", accept_none=True))
    )
    _name = field_validator("name")(classmethod(valstr("name")))
