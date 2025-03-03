from typing import Generic
from typing import Optional
from typing import TypeVar

from fastapi import HTTPException
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import ValidationError

T = TypeVar("T")


class PaginationRequest(BaseModel):

    page: int = Field(ge=1)
    page_size: Optional[int] = Field(ge=1)

    @model_validator(mode="after")
    def valid_pagination_parameters(self):
        if self.page_size is None and self.page > 1:
            raise ValueError(
                "Invalid pagination parameters: "
                f"page_size is None but page={self.page} is greater than 1."
            )
        return self


def get_pagination_params(
    page: int = 1, page_size: Optional[int] = None
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

    @model_validator(mode="after")
    def valid_page(self):

        if self.page_size == 0 and self.total_count > 0:
            raise ValueError(
                "'page_size' must be greater than 0 "
                "when 'total_count' is greater than 0."
            )
        if self.page_size > 0 and len(self.items) > self.page_size:
            raise ValueError(
                f"'items' list length ({len(self.items)}) "
                f"exceeds 'page_size' ({self.page_size})."
            )

        max_page = 1
        if self.page_size > 0 and self.total_count > 0:
            max_page = (
                self.total_count + self.page_size - 1
            ) // self.page_size

        if self.current_page == max_page and self.page_size > 0:
            expected_items_on_last_page = self.total_count % self.page_size
            if expected_items_on_last_page == 0 and self.total_count > 0:
                expected_items_on_last_page = self.page_size
            if len(self.items) > expected_items_on_last_page:
                raise ValueError(
                    f"Too many items on the last page: got {len(self.items)}, "
                    f"expected at most {expected_items_on_last_page}."
                )
        if self.current_page > max_page and len(self.items) > 0:
            raise ValueError(
                f"Current page must be empty: got {len(self.items)} items."
            )

        return self
