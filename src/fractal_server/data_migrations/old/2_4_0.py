import logging

from packaging.version import parse
from sqlalchemy import select

import fractal_server
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.security import FRACTAL_DEFAULT_GROUP_NAME


def _check_current_version(*, expected_version: str):
    # Check that this module matches with the current version
    module_version = parse(expected_version)
    current_version = parse(fractal_server.__VERSION__)
    if (
        current_version.major != module_version.major
        or current_version.minor != module_version.minor
        or current_version.micro != module_version.micro
    ):
        raise RuntimeError(
            f"{fractal_server.__VERSION__=} not matching with {__file__=}"
        )


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")
    _check_current_version(expected_version="2.4.0")

    with next(get_sync_db()) as db:
        # Find default group
        stm = select(UserGroup).where(
            UserGroup.name == FRACTAL_DEFAULT_GROUP_NAME
        )
        res = db.execute(stm)
        default_group = res.scalar_one_or_none()
        if default_group is None:
            raise RuntimeError("Default group not found, exit.")
        logger.warning(
            "Default user group exists: "
            f"{default_group.id=}, {default_group.name=}."
        )

        # Find
        stm = select(UserOAuth)
        users = db.execute(stm).scalars().unique().all()
        for user in sorted(users, key=lambda x: x.id):
            logger.warning(
                f"START adding {user.id=} ({user.email=}) to default group."
            )
            link = LinkUserGroup(user_id=user.id, group_id=default_group.id)
            db.add(link)
            db.commit()
            logger.warning(
                f"END   adding {user.id=} ({user.email=}) to default group."
            )

    logger.warning("END of execution of fix_db function")
