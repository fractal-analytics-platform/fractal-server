import logging

from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryRun
from fractal_server.app.models import TaskV2
from fractal_server.app.models import WorkflowTaskV2

logger = logging.getLogger("fix_db")
logger.setLevel(logging.INFO)


def fix_db():

    with next(get_sync_db()) as db:

        stm = select(HistoryRun).order_by(HistoryRun.id)
        history_runs = db.execute(stm).scalars().all()
        for hr in history_runs:
            wft = db.get(WorkflowTaskV2, hr.workflowtask_id)
            if wft is None:
                task_id = hr.workflowtask_dump.get("task_id")
                if task_id is not None and db.get(TaskV2, task_id) is not None:
                    hr.task_id = task_id
            else:
                hr.task_id = wft.task_id
            db.add(hr)
        db.commit()
