from typing import Any

from pydantic import BaseModel


class _BaseTask(BaseModel):
    class Config:
        arbitrary_types_allowed = True
        extra = "forbid"

    name: str
    executable: str
    meta: dict[str, Any] | None
    input_types: dict[str, bool] | None
    output_types: dict[str, bool] | None


class CompoundTask(_BaseTask):
    executable_init: str
    meta_init: dict[str, Any] | None

    @property
    def executable_non_parallel(self) -> str:
        return self.executable_init

    @property
    def executable_parallel(self) -> str:
        return self.executable

    @property
    def meta_non_parallel(self) -> dict[str, Any] | None:
        return self.meta_init

    @property
    def meta_parallel(self) -> dict[str, Any] | None:
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
    def meta_parallel(self) -> dict[str, Any] | None:
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
    def meta_non_parallel(self) -> dict[str, Any] | None:
        return self.meta
