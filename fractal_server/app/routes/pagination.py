from typing import Generic
from typing import TypeVar

from fastapi import HTTPException
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import ValidationError

T = TypeVar("T")


class PaginationRequest(BaseModel):

    page: int = Field(ge=1)
    page_size: int | None = Field(ge=1)

    @model_validator(mode="after")
    def valid_pagination_parameters(self):
        if self.page_size is None and self.page > 1:
            raise ValueError(
                f"page_size is None but page={self.page} is greater than 1."
            )
        return self


def get_pagination_params(
    page: int = 1, page_size: int | None = None
) -> PaginationRequest:
    try:
        pagination = PaginationRequest(page=page, page_size=page_size)
    except ValidationError as e:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid pagination parameters. Original error: '{e}'.",
        )
    return pagination


class PaginationResponse(BaseModel, Generic[T]):

    current_page: int = Field(ge=1)
    page_size: int = Field(ge=0)
    total_count: int = Field(ge=0)

    items: list[T]
