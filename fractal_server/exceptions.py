from typing import Any

from fastapi import HTTPException


class UnreachableBranchError(RuntimeError):
    """
    Exception marking a code branch that should have not been reached.
    """

    pass


class HTTPExceptionWithData(HTTPException):
    def __init__(
        self,
        status_code: int,
        data: Any,
        detail: str | None = None,
    ):
        self.data = data
        super().__init__(
            status_code=status_code,
            detail=f"{detail + ' ' if detail else ''}[HAS_ERROR_DATA]",
        )

    def __repr__(self) -> str:
        return (
            "HTTPExceptionWithData("
            f"status_code={self.status_code!r},"
            f"detail={self.detail!r},"
            f"data={self.data!r}"
            ")"
        )
