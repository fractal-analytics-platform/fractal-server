from typing import Any
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import HttpUrl
from pydantic import model_validator

from .._validators import NonEmptyString


class TaskManifestV2(BaseModel):
    """
    Represents a task within a V2 manifest.

    Attributes:
        name:
            The task name
        executable:
            Path to the executable relative to the package root

            Note: by package root we mean "as it will be installed". If a
            package `Pkg` installs in the folder `pkg` the executable
            `pkg/executable.py`, this attribute must contain only
            `executable.py`.
        input_type:
            The input type accepted by the task
        output_type:
            The output type returned by the task
        meta:
            Additional information about the package, such as hash of the
            executable, specific runtime requirements (e.g., need_gpu=True),
            etc.
        args_schema:
            JSON Schema for task arguments
        docs_info:
            Additional information about the Task, coming from the docstring.
        docs_link:
            Link to Task docs.
    """

    name: str
    executable_non_parallel: Optional[str] = None
    executable_parallel: Optional[str] = None
    input_types: dict[str, bool] = Field(default_factory=dict)
    output_types: dict[str, bool] = Field(default_factory=dict)
    meta_non_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: dict[str, Any] = Field(default_factory=dict)
    args_schema_non_parallel: Optional[dict[str, Any]] = None
    args_schema_parallel: Optional[dict[str, Any]] = None
    docs_info: Optional[str] = None
    docs_link: Optional[str] = None

    category: Optional[str] = None
    modality: Optional[str] = None
    tags: list[str] = Field(default_factory=list)

    type: Optional[
        Literal[
            "compound",
            "converter_compound",
            "non_parallel",
            "converter_non_parallel",
            "parallel",
        ]
    ] = None

    @model_validator(mode="after")
    def validate_executable_args_meta(self):
        executable_non_parallel = self.executable_non_parallel
        executable_parallel = self.executable_parallel
        if (executable_non_parallel is None) and (executable_parallel is None):
            raise ValueError(
                "`TaskManifestV2.executable_non_parallel` and "
                "`TaskManifestV2.executable_parallel` cannot be both None."
            )

        elif executable_non_parallel is None:
            meta_non_parallel = self.meta_non_parallel
            if meta_non_parallel != {}:
                raise ValueError(
                    "`TaskManifestV2.meta_non_parallel` must be an empty dict "
                    "if `TaskManifestV2.executable_non_parallel` is None. "
                    f"Given: {meta_non_parallel}."
                )

            args_schema_non_parallel = self.args_schema_non_parallel
            if args_schema_non_parallel is not None:
                raise ValueError(
                    "`TaskManifestV2.args_schema_non_parallel` must be None "
                    "if `TaskManifestV2.executable_non_parallel` is None. "
                    f"Given: {args_schema_non_parallel}."
                )

        elif executable_parallel is None:
            meta_parallel = self.meta_parallel
            if meta_parallel != {}:
                raise ValueError(
                    "`TaskManifestV2.meta_parallel` must be an empty dict if "
                    "`TaskManifestV2.executable_parallel` is None. "
                    f"Given: {meta_parallel}."
                )

            args_schema_parallel = self.args_schema_parallel
            if args_schema_parallel is not None:
                raise ValueError(
                    "`TaskManifestV2.args_schema_parallel` must be None if "
                    "`TaskManifestV2.executable_parallel` is None. "
                    f"Given: {args_schema_parallel}."
                )

        return self

    @field_validator("docs_link", mode="after")
    @classmethod
    def validate_docs_link(cls, value):
        if value is not None:
            HttpUrl(value)
        return value


class ManifestV2(BaseModel):
    """
    Packages containing tasks are required to include a special file
    `__FRACTAL_MANIFEST__.json` in order to be discovered and used by Fractal.

    This model class and the model classes it depends on provide the base
    schema to read, write and validate manifests.

    Attributes:
        manifest_version:
            A version string that provides indication for compatibility between
            manifests as the schema evolves. This is for instance used by
            Fractal to determine which subclass of the present base class needs
            be used to read and validate the input.
        task_list : list[TaskManifestType]
            The list of tasks, represented as specified by subclasses of the
            _TaskManifestBase (a.k.a. TaskManifestType)
        has_args_schemas:
            `True` if the manifest includes JSON Schemas for the arguments of
            each task.
        args_schema_version:
            Label of how `args_schema`s were generated (e.g. `pydantic_v1`).
    """

    manifest_version: str
    task_list: list[TaskManifestV2]
    has_args_schemas: bool = False
    args_schema_version: Optional[str] = None
    authors: Optional[NonEmptyString] = None

    @model_validator(mode="after")
    def _check_args_schemas_are_present(self):
        has_args_schemas = self.has_args_schemas
        task_list = self.task_list
        if has_args_schemas is True:
            for task in task_list:
                if task.executable_parallel is not None:
                    if task.args_schema_parallel is None:
                        raise ValueError(
                            f"Manifest has {has_args_schemas=}, but "
                            f"task '{task.name}' has "
                            f"{task.args_schema_parallel=}."
                        )
                if task.executable_non_parallel is not None:
                    if task.args_schema_non_parallel is None:
                        raise ValueError(
                            f"Manifest has {has_args_schemas=}, but "
                            f"task '{task.name}' has "
                            f"{task.args_schema_non_parallel=}."
                        )
        return self

    @model_validator(mode="after")
    def _unique_task_names(self):
        task_list = self.task_list
        task_list_names = [t.name for t in task_list]
        if len(set(task_list_names)) != len(task_list_names):
            raise ValueError(
                (
                    "Task names in manifest must be unique.\n",
                    f"Given: {task_list_names}.",
                )
            )
        return self

    @field_validator("manifest_version")
    @classmethod
    def manifest_version_2(cls, value):
        if value != "2":
            raise ValueError(f"Wrong manifest version (given {value})")
        return value
