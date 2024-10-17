from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field
from pydantic import HttpUrl
from pydantic import root_validator
from pydantic import validator

from fractal_server.app.schemas._validators import val_unique_list
from fractal_server.app.schemas._validators import valdictkeys
from fractal_server.app.schemas._validators import valstr
from fractal_server.string_tools import validate_cmd


class TaskCreateV2(BaseModel, extra=Extra.forbid):

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
    docs_link: Optional[HttpUrl] = None

    input_types: dict[str, bool] = Field(default={})
    output_types: dict[str, bool] = Field(default={})

    category: Optional[str] = None
    modality: Optional[str] = None
    tags: list[str] = Field(default_factory=list)
    authors: Optional[str] = None

    # Validators
    @root_validator
    def validate_commands(cls, values):
        command_parallel = values.get("command_parallel")
        command_non_parallel = values.get("command_non_parallel")
        if (command_parallel is None) and (command_non_parallel is None):
            raise ValueError(
                "Task must have at least one valid command "
                "(parallel and/or non_parallel)"
            )
        if command_parallel is not None:
            validate_cmd(command_parallel)
        if command_non_parallel is not None:
            validate_cmd(command_non_parallel)

        return values

    _name = validator("name", allow_reuse=True)(valstr("name"))
    _command_non_parallel = validator(
        "command_non_parallel", allow_reuse=True
    )(valstr("command_non_parallel"))
    _command_parallel = validator("command_parallel", allow_reuse=True)(
        valstr("command_parallel")
    )
    _version = validator("version", allow_reuse=True)(valstr("version"))

    _meta_non_parallel = validator("meta_non_parallel", allow_reuse=True)(
        valdictkeys("meta_non_parallel")
    )
    _meta_parallel = validator("meta_parallel", allow_reuse=True)(
        valdictkeys("meta_parallel")
    )
    _args_schema_non_parallel = validator(
        "args_schema_non_parallel", allow_reuse=True
    )(valdictkeys("args_schema_non_parallel"))
    _args_schema_parallel = validator(
        "args_schema_parallel", allow_reuse=True
    )(valdictkeys("args_schema_parallel"))
    _args_schema_version = validator("args_schema_version", allow_reuse=True)(
        valstr("args_schema_version")
    )
    _input_types = validator("input_types", allow_reuse=True)(
        valdictkeys("input_types")
    )
    _output_types = validator("output_types", allow_reuse=True)(
        valdictkeys("output_types")
    )

    _category = validator("category", allow_reuse=True)(
        valstr("category", accept_none=True)
    )
    _modality = validator("modality", allow_reuse=True)(
        valstr("modality", accept_none=True)
    )
    _authors = validator("authors", allow_reuse=True)(
        valstr("authors", accept_none=True)
    )

    @validator("tags")
    def validate_list_of_strings(cls, value):
        for i, tag in enumerate(value):
            value[i] = valstr(f"tags[{i}]")(tag)
        return val_unique_list("tags")(value)


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
    docs_link: Optional[HttpUrl] = None
    input_types: dict[str, bool]
    output_types: dict[str, bool]

    taskgroupv2_id: Optional[int] = None

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: list[str]


class TaskUpdateV2(BaseModel, extra=Extra.forbid):

    command_parallel: Optional[str] = None
    command_non_parallel: Optional[str] = None
    input_types: Optional[dict[str, bool]] = None
    output_types: Optional[dict[str, bool]] = None

    category: Optional[str] = None
    modality: Optional[str] = None
    authors: Optional[str] = None
    tags: Optional[list[str]] = None

    # Validators
    @validator("input_types", "output_types")
    def val_is_dict(cls, v):
        if not isinstance(v, dict):
            raise ValueError
        return v

    _command_parallel = validator("command_parallel", allow_reuse=True)(
        valstr("command_parallel")
    )
    _command_non_parallel = validator(
        "command_non_parallel", allow_reuse=True
    )(valstr("command_non_parallel"))
    _input_types = validator("input_types", allow_reuse=True)(
        valdictkeys("input_types")
    )
    _output_types = validator("output_types", allow_reuse=True)(
        valdictkeys("output_types")
    )

    _category = validator("category", allow_reuse=True)(
        valstr("category", accept_none=True)
    )
    _modality = validator("modality", allow_reuse=True)(
        valstr("modality", accept_none=True)
    )
    _authors = validator("authors", allow_reuse=True)(
        valstr("authors", accept_none=True)
    )

    @validator("tags")
    def validate_tags(cls, value):
        for i, tag in enumerate(value):
            value[i] = valstr(f"tags[{i}]")(tag)
        return val_unique_list("tags")(value)


class TaskImportV2(BaseModel, extra=Extra.forbid):

    pkg_name: str
    version: Optional[str] = None
    name: str
    _pkg_name = validator("pkg_name", allow_reuse=True)(valstr("pkg_name"))
    _version = validator("version", allow_reuse=True)(
        valstr("version", accept_none=True)
    )
    _name = validator("name", allow_reuse=True)(valstr("name"))


class TaskImportV2Legacy(BaseModel):
    source: str
    _source = validator("source", allow_reuse=True)(valstr("source"))


class TaskExportV2(BaseModel):

    pkg_name: str
    version: Optional[str] = None
    name: str

    _pkg_name = validator("pkg_name", allow_reuse=True)(valstr("pkg_name"))
    _version = validator("version", allow_reuse=True)(
        valstr("version", accept_none=True)
    )
    _name = validator("name", allow_reuse=True)(valstr("name"))
