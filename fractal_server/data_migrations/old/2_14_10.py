import logging

from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryRun
from fractal_server.app.models import TaskV2
from fractal_server.app.models import WorkflowTaskV2

logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def fix_db():
    logger.info("START execution of fix_db function")

    with next(get_sync_db()) as db:
        stm = select(HistoryRun).order_by(HistoryRun.id)
        history_runs = db.execute(stm).scalars().all()

        for hr in history_runs:
            logger.info(f"HistoryRun[{hr.id}] START")
            if hr.workflowtask_id is None:
                continue
            wft = db.get(WorkflowTaskV2, hr.workflowtask_id)
            if wft is None:
                logger.warning(
                    f"WorkflowTaskV2[{hr.workflowtask_id}] not found. "
                    "Trying to use HistoryRun.workflowtask_dump"
                )
                task_id = hr.workflowtask_dump.get("task_id")
                if task_id is not None and db.get(TaskV2, task_id) is not None:
                    hr.task_id = task_id
                else:
                    logger.warning(f"TaskV2[{task_id}] not found")
            else:
                hr.task_id = wft.task_id
                logger.info(
                    f"HistoryRun[{hr.id}].task_id set to {wft.task_id}"
                )

            db.add(hr)
            logger.info(f"HistoryRun[{hr.id}] END")

        db.commit()
        logger.info("Changes committed.")

    logger.info("END execution of fix_db function")
