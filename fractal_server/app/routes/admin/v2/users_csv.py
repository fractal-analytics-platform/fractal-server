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
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import Profile
from fractal_server.app.models import ProjectV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.logger import set_logger

router = APIRouter()


logger = set_logger(__name__)


@router.get("/", response_class=StreamingResponse)
async def list_users(
    exclude_zero_jobs: bool = False,
    start_timestamp_min: AwareDatetime | None = None,
    start_timestamp_max: AwareDatetime | None = None,
    superuser: UserOAuth = Depends(current_superuser_act),
    db: AsyncSession = Depends(get_async_db),
) -> StreamingResponse:
    SEPARATOR = "|"
    stm_num_job = (
        select(func.count(JobV2.id))
        .join(ProjectV2, ProjectV2.id == JobV2.project_id)
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.user_id == UserOAuth.id)
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
        select(func.aggregate_strings(UserGroup.name, SEPARATOR))
        .select_from(
            join(
                LinkUserGroup,
                UserGroup,
                LinkUserGroup.group_id == UserGroup.id,
            )
        )
        .where(LinkUserGroup.user_id == UserOAuth.id)
    )

    """
    Columns:
        ID
        email
        SLURM username
        SLURM accounts
        project_dirs
        user groups
        #jobs
    """
    stm = (
        select(
            Bundle(
                "placeholder",
                UserOAuth.id,
                UserOAuth.email,
                Profile.username,
                func.array_to_string(UserOAuth.slurm_accounts, SEPARATOR),
                func.array_to_string(UserOAuth.project_dirs, SEPARATOR),
                stm_user_groups.scalar_subquery(),
                stm_num_job.scalar_subquery(),
            ),
        )
        .join(Profile, Profile.id == UserOAuth.profile_id)
        .order_by(UserOAuth.email)
    )
    res = await db.execute(stm)
    users = res.scalars().all()

    # Python post-processing to apply `exclude_zero_jobs`
    if exclude_zero_jobs:
        users = [row for row in users if row[-1] > 0]

    with io.StringIO() as output:
        writer = csv.writer(output)
        writer.writerows(users)
        csv_string = output.getvalue()

    return StreamingResponse(
        iter(csv_string),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=users.csv"},
    )
