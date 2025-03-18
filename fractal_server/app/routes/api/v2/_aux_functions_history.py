from pathlib import Path

from fastapi import HTTPException
from fastapi import status

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import WorkflowTaskV2
from fractal_server.app.models.v2 import HistoryUnit


async def get_history_unit_or_404(
    *, history_unit_id: int, db: AsyncSession
) -> HistoryUnit:
    """
    Get an existing HistoryUnit  or raise a 404.

    Arguments:
        history_unit_id: The `HistoryUnit` id
        db: An asynchronous db session
    """
    history_unit = await db.get(HistoryUnit, history_unit_id)
    if history_unit is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"HistoryUnit {history_unit_id} not found",
        )
    return history_unit


def read_log_file(
    *,
    logfile: str | None,
    wftask: WorkflowTaskV2,
    dataset_id: int,
):
    if logfile is None or not Path(logfile).exists():
        return (
            f"Logs for task '{wftask.task.name}' in dataset "
            f"{dataset_id} are not available."
        )

    try:
        with open(logfile, "r") as f:
            return f.read()
    except Exception as e:
        return (
            f"Error while retrieving logs for task '{wftask.task.name}' "
            f"in dataset {dataset_id}. Original error: {str(e)}."
        )
