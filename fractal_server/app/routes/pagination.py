from typing import Any
from typing import Generic
from typing import TypeVar

from fastapi import HTTPException
from pydantic import BaseModel
from pydantic import Field
from pydantic import ValidationError
from pydantic import model_validator
from sqlmodel.sql.expression import Select
from sqlmodel.sql.expression import SelectOfScalar

from fractal_server.app.db import AsyncSession

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


class PaginationData(BaseModel):
    stm: Any
    page: int
    page_size: int
    total_count: int


async def get_pagination_data(
    *,
    stm: Select | SelectOfScalar,
    stm_count: SelectOfScalar[int],
    pagination: PaginationRequest,
    db: AsyncSession,
) -> PaginationData:
    """
    Prepare pagination metadata (page, page_size and total_count) and apply
    offset/limit to the query statement.
    """
    res_total_count = await db.execute(stm_count)
    total_count = res_total_count.scalar()

    if pagination.page_size is not None:
        page_size = pagination.page_size
        stm = stm.offset((pagination.page - 1) * page_size).limit(page_size)
    else:
        page_size = total_count

    return PaginationData(
        stm=stm,
        page=pagination.page,
        page_size=page_size,
        total_count=total_count,
    )


async def get_paginated_response(
    *,
    stm: SelectOfScalar[T],
    stm_count: SelectOfScalar[int],
    pagination: PaginationRequest,
    db: AsyncSession,
) -> PaginationResponse[T]:
    """
    Execute a paginated query and return a structured response.

    This only applies to `SelectOfScalar[T]` statements, i.e. applies to
    `select(X)` but not to `select(X, Y)`.
    """
    pagination_data = await get_pagination_data(
        stm=stm,
        stm_count=stm_count,
        pagination=pagination,
        db=db,
    )

    res = await db.execute(pagination_data.stm)
    records = res.scalars().all()

    return PaginationResponse[T](
        total_count=pagination_data.total_count,
        page_size=pagination_data.page_size,
        current_page=pagination_data.page,
        items=records,
    )
