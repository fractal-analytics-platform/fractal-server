from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import model_validator

from fractal_server.types import AbsolutePathStr
from fractal_server.types import NonEmptyStr


class TasksPythonSettings(BaseModel):
    """
    Configuration for the Python base interpreters to be used for task venvs.

    For task collection to work, there must be one or more base Python
    interpreters available on your system.
    """

    default_version: NonEmptyStr
    """
    Default task-collection Python version (must be a key of `versions`).
    """
    versions: dict[
        Literal[
            "3.9",
            "3.10",
            "3.11",
            "3.12",
            "3.13",
            "3.14",
        ],
        AbsolutePathStr,
    ]
    """
    Dictionary mapping Python versions to the corresponding interpreters.
    """

    pip_cache_dir: AbsolutePathStr | None = None
    """
    Argument for `--cache-dir` option of `pip install`, if set.
    """

    @model_validator(mode="after")
    def _validate_versions(self) -> Self:
        if self.default_version not in self.versions.keys():
            raise ValueError(
                f"The default Python version ('{self.default_version}') is "
                f"not available in {list(self.versions.keys())}."
            )

        return self
