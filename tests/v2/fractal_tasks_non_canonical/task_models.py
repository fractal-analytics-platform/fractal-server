from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict


class _BaseTask(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")

    name: str
    executable: str
    meta: Optional[dict[str, Any]]
    input_types: Optional[dict[str, bool]]
    output_types: Optional[dict[str, bool]]


class CompoundTask(_BaseTask):
    executable_init: str
    meta_init: Optional[dict[str, Any]] = None

    @property
    def executable_non_parallel(self) -> str:
        return self.executable_init

    @property
    def executable_parallel(self) -> str:
        return self.executable

    @property
    def meta_non_parallel(self) -> Optional[dict[str, Any]]:
        return self.meta_init

    @property
    def meta_parallel(self) -> Optional[dict[str, Any]]:
        return self.meta


class ParallelTask(_BaseTask):
    @property
    def executable_non_parallel(self) -> None:
        return None

    @property
    def executable_parallel(self) -> str:
        return self.executable

    @property
    def meta_non_parallel(self) -> None:
        return None

    @property
    def meta_parallel(self) -> Optional[dict[str, Any]]:
        return self.meta


class NonParallelTask(_BaseTask):
    @property
    def executable_parallel(self) -> None:
        return None

    @property
    def executable_non_parallel(self) -> str:
        return self.executable

    @property
    def meta_parallel(self) -> None:
        return None

    @property
    def meta_non_parallel(self) -> Optional[dict[str, Any]]:
        return self.meta
