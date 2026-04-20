"""
Definition of `/auth/users/csv/` route.
"""

import csv
import io

from fastapi import APIRouter
from fastapi import Depends
from fastapi.responses import StreamingResponse
from pydantic.types import AwareDatetime
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Bundle
from sqlalchemy.orm import join
from sqlmodel import select

from fractal_server.app.db import get_async_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import Profile
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.auth import current_superuser_act

router = APIRouter()

_COLUMN_NAMES = (
    "id",
    "email",
    "slurm_username",
    "slurm_accounts",
    "project_dirs",
    "user_groups",
    "num_jobs",
)
_ARRAY_SEPARATOR = "|"


@router.get("/", response_class=StreamingResponse)
async def list_users(
    exclude_zero_jobs: bool = False,
    start_timestamp_min: AwareDatetime | None = None,
    start_timestamp_max: AwareDatetime | None = None,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    """
    Provide csv table of users and some of their properties.
    """
    stm_num_job = select(func.count(JobV2.id)).where(
        JobV2.user_email == UserOAuth.email
    )
    if start_timestamp_min is not None:
        stm_num_job = stm_num_job.where(
            JobV2.start_timestamp >= start_timestamp_min
        )
    if start_timestamp_max is not None:
        stm_num_job = stm_num_job.where(
            JobV2.start_timestamp <= start_timestamp_max
        )
    stm_user_groups = (
        select(func.aggregate_strings(UserGroup.name, _ARRAY_SEPARATOR))
        .select_from(
            join(
                LinkUserGroup,
                UserGroup,
                LinkUserGroup.group_id == UserGroup.id,
            )
        )
        .where(LinkUserGroup.user_id == UserOAuth.id)
    )

    stm = (
        select(
            Bundle(
                "my-bundle",
                UserOAuth.id,
                UserOAuth.email,
                Profile.username,
                func.array_to_string(
                    UserOAuth.slurm_accounts,
                    _ARRAY_SEPARATOR,
                ),
                func.array_to_string(
                    UserOAuth.project_dirs,
                    _ARRAY_SEPARATOR,
                ),
                stm_user_groups.scalar_subquery(),
                stm_num_job.scalar_subquery(),
            ),
        )
        .join(Profile, Profile.id == UserOAuth.profile_id)
        .order_by(UserOAuth.email)
    )
    res = await db.execute(stm)
    users = res.scalars().all()

    # Exclude users without jobs in the given time interval
    if exclude_zero_jobs:
        users = [row for row in users if row[-1] > 0]

    with io.StringIO() as output:
        writer = csv.writer(output)
        writer.writerow(_COLUMN_NAMES)
        writer.writerows(users)
        csv_string = output.getvalue()

    return StreamingResponse(
        csv_string,
        media_type="text/csv",
    )
