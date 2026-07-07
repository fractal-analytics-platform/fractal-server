import shutil
import subprocess  # nosec
from functools import cache
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DataError
from sqlalchemy.orm import Session
from sqlmodel import select
from sqlmodel import update

from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.schemas.v2 import HistoryUnitStatus
from fractal_server.logger import set_logger

_CHUNK_SIZE = 2_000

logger = set_logger(__name__)


@cache
def _get_grep_path() -> str:
    return shutil.which("grep")


def update_status_of_history_run(
    *,
    history_run_id: int,
    status: HistoryUnitStatus,
    db_sync: Session,
) -> None:
    run = db_sync.get_one(HistoryRun, history_run_id)
    run.status = status
    db_sync.merge(run)
    db_sync.commit()


def update_history_unit_no_commit(
    *,
    history_unit_id: int,
    status: HistoryUnitStatus,
    db_sync: Session,
) -> None:
    unit = db_sync.get_one(HistoryUnit, history_unit_id)
    unit.status = status
    res = subprocess.run(  # nosec
        [_get_grep_path(), "-i", "WARNING", "-q", unit.logfile],
        stderr=subprocess.DEVNULL,
    )
    unit.has_warnings = res.returncode == 0
    db_sync.merge(unit)
    db_sync.flush()


def bulk_update_has_warnings_history_unit(
    *,
    history_unit_ids: list[int],
    db_sync: Session,
) -> None:
    ids_logfiles = db_sync.execute(
        select(HistoryUnit.id, HistoryUnit.logfile).where(
            HistoryUnit.id.in_(history_unit_ids)
        )
    ).all()
    grep_path = _get_grep_path()
    units_with_warnings = [
        _id
        for _id, logfile in ids_logfiles
        if subprocess.run(  # nosec
            [grep_path, "-i", "WARNING", "-q", logfile],
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    ]
    len_units_with_warnings = len(units_with_warnings)
    for ind in range(0, len_units_with_warnings, _CHUNK_SIZE):
        db_sync.execute(
            update(HistoryUnit)
            .where(
                HistoryUnit.id.in_(units_with_warnings[ind : ind + _CHUNK_SIZE])
            )
            .values(has_warnings=True)
        )
        db_sync.commit()


def bulk_update_status_of_history_unit(
    *,
    history_unit_ids: list[int],
    status: HistoryUnitStatus,
    db_sync: Session,
) -> None:
    len_history_unit_ids = len(history_unit_ids)
    logger.debug(
        f"[bulk_update_status_of_history_unit] {len_history_unit_ids=}."
    )
    for ind in range(0, len_history_unit_ids, _CHUNK_SIZE):
        db_sync.execute(
            update(HistoryUnit)
            .where(
                HistoryUnit.id.in_(history_unit_ids[ind : ind + _CHUNK_SIZE])
            )
            .values(status=status)
        )
        # NOTE: keeping commit within the for loop is much more efficient
        db_sync.commit()


def bulk_upsert_image_cache_fast(
    *,
    list_upsert_objects: list[dict[str, Any]],
    db: Session,
) -> None:
    """
    Insert or update many objects into `HistoryImageCache` and commit

    This function is an optimized version of

    ```python
    for obj in list_upsert_objects:
        db.merge(**obj)
    db.commit()
    ```

    See docs at
    https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#insert-on-conflict-upsert

    NOTE: we tried to replace `index_elements` with
    `constraint="pk_historyimagecache"`, but it did not work as expected.

    Args:
        list_upsert_objects:
            List of dictionaries for objects to be upsert-ed.
        db: A sync database session
    """
    len_list_upsert_objects = len(list_upsert_objects)

    logger.debug(f"[bulk_upsert_image_cache_fast] {len_list_upsert_objects=}.")

    if len_list_upsert_objects == 0:
        return None

    for ind in range(0, len_list_upsert_objects, _CHUNK_SIZE):
        stmt = pg_insert(HistoryImageCache).values(
            list_upsert_objects[ind : ind + _CHUNK_SIZE]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                HistoryImageCache.zarr_url,
                HistoryImageCache.dataset_id,
                HistoryImageCache.workflowtask_id,
            ],
            set_=dict(
                latest_history_unit_id=stmt.excluded.latest_history_unit_id
            ),
        )
        db.execute(stmt)
        db.commit()


def update_executor_error_log_safe(
    *,
    job_id: int,
    executor_error_log: str | None,
    db: Session,
) -> None:
    """
    Update `JobV2.executor_error_log` with a `DataError` fallback.
    """
    job_db = db.get_one(JobV2, job_id)
    job_db.executor_error_log = executor_error_log
    try:
        db.merge(job_db)
        db.commit()
    except DataError as exc:
        logger.warning(
            f"Cannot update `executor_error_log` for job {job_id}, due to {exc}"
        )
        db.rollback()
