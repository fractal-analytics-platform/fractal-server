import logging

from packaging.version import parse
from sqlalchemy import select

import fractal_server
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2


def fix_db():
    logger = logging.getLogger("fix_db")
    logger.warning("START execution of fix_db function")

    # Check that this module matches with the current version
    module_version = parse("2.23.0")
    current_version = parse(fractal_server.__VERSION__)
    if (
        current_version.major != module_version.major
        or current_version.minor != module_version.minor
        or current_version.micro != module_version.micro
    ):
        raise RuntimeError(
            f"{fractal_server.__VERSION__=} not matching with {__file__=}"
        )

    with next(get_sync_db()) as db:
        res = db.execute(select(TaskV2).where(TaskV2.version.is_(None)))
        for task in res.scalars().all():
            task.version = "0"
            db.add(task)

        res = db.execute(
            select(TaskGroupV2).where(TaskGroupV2.version.is_(None))
        )
        for taskgroup in res.scalars().all():
            taskgroup.version = "0"
            db.add(taskgroup)
        db.commit()

    logger.warning("END of execution of fix_db function")
