import itertools

from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.exceptions import UnreachableBranchError
from fractal_server.logger import set_logger


logger = set_logger(__name__)


async def _disambiguate_task_groups(
    *,
    matching_task_groups: list[TaskGroupV2],
    user_id: int,
    default_group_id: int,
    db: AsyncSession,
) -> TaskGroupV2 | None:
    """
    Find ownership-based top-priority task group, if any.

    Args:
        matching_task_groups:
        user_id:
        default_group_id:
        db:

    Returns:
        The task group or `None`.
    """

    # Highest priority: task groups created by user
    list_user_ids = [tg.user_id for tg in matching_task_groups]
    try:
        ind_user_id = list_user_ids.index(user_id)
        task_group = matching_task_groups[ind_user_id]
        logger.debug(
            "[_disambiguate_task_groups] "
            f"Found task group {task_group.id} with {user_id=}, return."
        )
        return task_group
    except ValueError:
        logger.debug(
            "[_disambiguate_task_groups] "
            f"No task group with {user_id=}, continue."
        )

    # Medium priority: task groups owned by default user group
    list_user_group_ids = [tg.user_group_id for tg in matching_task_groups]
    try:
        ind_user_group_id = list_user_group_ids.index(default_group_id)
        task_group = matching_task_groups[ind_user_group_id]
        logger.debug(
            "[_disambiguate_task_groups] "
            f"Found task group {task_group.id} with {user_id=}, return."
        )
        return task_group
    except ValueError:
        logger.debug(
            "[_disambiguate_task_groups] "
            "No task group with user_group_id="
            f"{default_group_id}, continue."
        )

    # Lowest priority: task groups owned by other groups, sorted
    # according to age of the user/usergroup link
    logger.debug(
        "[_disambiguate_task_groups] "
        "Sort remaining task groups by oldest-user-link."
    )
    stm = (
        select(LinkUserGroup.group_id)
        .where(LinkUserGroup.user_id == user_id)
        .where(LinkUserGroup.group_id.in_(list_user_group_ids))
        .order_by(LinkUserGroup.timestamp_created.asc())
    )
    res = await db.execute(stm)
    oldest_user_group_id = res.scalars().first()
    logger.debug(
        "[_disambiguate_task_groups] " f"Result: {oldest_user_group_id=}."
    )
    task_group = next(
        iter(
            task_group
            for task_group in matching_task_groups
            if task_group.user_group_id == oldest_user_group_id
        ),
        None,
    )
    return task_group


async def _disambiguate_task_groups_not_none(
    *,
    matching_task_groups: list[TaskGroupV2],
    user_id: int,
    default_group_id: int,
    db: AsyncSession,
) -> TaskGroupV2:
    """
    Find ownership-based top-priority task group, and fail otherwise.

    Args:
        matching_task_groups:
        user_id:
        default_group_id:
        db:

    Returns:
        The top-priority task group.
    """
    task_group = await _disambiguate_task_groups(
        matching_task_groups=matching_task_groups,
        user_id=user_id,
        default_group_id=default_group_id,
        db=db,
    )
    if task_group is None:
        error_msg = (
            "[_disambiguate_task_groups_not_none] Could not find a task "
            f"group ({user_id=}, {default_group_id=})."
        )
        logger.error(f"UnreachableBranchError {error_msg}")
        raise UnreachableBranchError(error_msg)
    else:
        return task_group


async def remove_duplicate_task_groups(
    *,
    task_groups: list[TaskGroupV2],
    user_id: int,
    default_group_id: int,
    db: AsyncSession,
) -> list[TaskGroupV2]:
    """
    Extract an item for each `version` from a *sorted* task-group list.

    Args:
        task_groups: A list of task groups with identical `pkg_name`
        user_id: User ID

    Returns:
        New list of task groups with no duplicate `(pkg_name,version)` entries
    """

    new_task_groups = [
        (
            await _disambiguate_task_groups_not_none(
                matching_task_groups=list(groups),
                user_id=user_id,
                default_group_id=default_group_id,
                db=db,
            )
        )
        for version, groups in itertools.groupby(
            task_groups, key=lambda tg: tg.version
        )
    ]
    return new_task_groups
