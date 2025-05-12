from fastapi import HTTPException
from fastapi import status
from httpx import AsyncClient
from httpx import TimeoutException
from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.logger import set_logger


logger = set_logger(__name__)


async def get_package_version_from_pypi(
    name: str,
    version: str | None = None,
) -> str:
    """
    Make a GET call to PyPI JSON API and get latest *compatible* version.

    There are three cases:

    1. `version` is set and it is found on PyPI as-is.
    2. `version` is set but it is not found on PyPI as-is.
    3. `version` is unset, and we query `PyPI` for latest.

    Ref https://warehouse.pypa.io/api-reference/json.html.

    Arguments:
        name: Package name.
        version:
            Could be a correct version (`1.3.0`), an incomplete one
            (`1.3`) or `None`.
    """

    url = f"https://pypi.org/pypi/{name}/json"
    hint = f"Hint: specify the required version for '{name}'."

    # Make request to PyPI
    try:
        async with AsyncClient(timeout=5.0) as client:
            res = await client.get(url)
    except TimeoutException as e:
        error_msg = (
            f"A TimeoutException occurred while getting {url}.\n"
            f"Original error: {str(e)}."
        )
        logger.warning(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_msg,
        )
    except BaseException as e:
        error_msg = (
            f"An unknown error occurred while getting {url}. "
            f"Original error: {str(e)}."
        )
        logger.warning(error_msg)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=error_msg,
        )

    # Parse response
    if res.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Could not get {url} (status_code {res.status_code})."
                f"\n{hint}"
            ),
        )
    try:
        response_data = res.json()
        latest_version = response_data["info"]["version"]
        available_releases = response_data["releases"].keys()
    except KeyError as e:
        logger.warning(
            f"A KeyError occurred while getting {url}. "
            f"Original error: {str(e)}."
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"A KeyError error occurred while getting {url}.\n{hint}",
        )

    logger.info(
        f"Obtained data from {url}: "
        f"{len(available_releases)} releases, "
        f"latest={latest_version}."
    )

    if version is not None:
        if version in available_releases:
            logger.info(f"Requested {version=} available on PyPI.")
            # Case 1: `version` is set and it is found on PyPI as-is
            return version
        else:
            # Case 2: `version` is set but it is not found on PyPI as-is
            # Filter using `version` as prefix, and sort
            matching_versions = [
                v for v in available_releases if v.startswith(version)
            ]
            logger.info(
                f"Requested {version=} not available on PyPI, "
                f"found {len(matching_versions)} versions matching "
                f"`{version}*`."
            )
            if len(matching_versions) == 0:
                logger.info(f"No version starting with {version} found.")
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=(
                        f"No version starting with {version} found.\n"
                        f"{hint}"
                    ),
                )
            else:
                latest_matching_version = sorted(matching_versions)[-1]
                return latest_matching_version
    else:
        # Case 3: `version` is unset and we use latest
        logger.info(f"No version requested, returning {latest_version=}.")
        return latest_version


async def check_no_ongoing_activity(
    *,
    task_group_id: int,
    db: AsyncSession,
) -> None:
    """
    Find ongoing activities for the same task group.

    Arguments:
        task_group_id:
        db:
    """
    # DB query
    stm = (
        select(TaskGroupActivityV2)
        .where(TaskGroupActivityV2.taskgroupv2_id == task_group_id)
        .where(TaskGroupActivityV2.status == TaskGroupActivityStatusV2.ONGOING)
    )
    res = await db.execute(stm)
    ongoing_activities = res.scalars().all()

    if ongoing_activities == []:
        # All good, exit
        return

    msg = "Found ongoing activities for the same task-group:"
    for ind, activity in enumerate(ongoing_activities):
        msg = (
            f"{msg}\n{ind + 1}) "
            f"Action={activity.action}, "
            f"status={activity.status}, "
            f"timestamp_started={activity.timestamp_started}."
        )
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        detail=msg,
    )


async def check_no_submitted_job(
    *,
    task_group_id: int,
    db: AsyncSession,
) -> None:
    """
    Find submitted jobs which include tasks from a given task group.

    Arguments:
        task_group_id: ID of the `TaskGroupV2` object.
        db: Asynchronous database session.
    """
    stm = (
        select(func.count(JobV2.id))
        .join(WorkflowV2, JobV2.workflow_id == WorkflowV2.id)
        .join(WorkflowTaskV2, WorkflowTaskV2.workflow_id == WorkflowV2.id)
        .join(TaskV2, WorkflowTaskV2.task_id == TaskV2.id)
        .where(WorkflowTaskV2.order >= JobV2.first_task_index)
        .where(WorkflowTaskV2.order <= JobV2.last_task_index)
        .where(JobV2.status == JobStatusTypeV2.SUBMITTED)
        .where(TaskV2.taskgroupv2_id == task_group_id)
    )
    res = await db.execute(stm)
    num_submitted_jobs = res.scalar()
    if num_submitted_jobs > 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Cannot act on task group because {num_submitted_jobs} "
                "submitted jobs use its tasks."
            ),
        )
