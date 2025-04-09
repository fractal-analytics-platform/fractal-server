from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session
from sqlmodel import update

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import HistoryImageCache
from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.schemas.v2 import HistoryUnitStatus

_CHUNK_SIZE = 2_000


def update_status_of_history_run(
    *,
    history_run_id: int,
    status: HistoryUnitStatus,
    db_sync: Session,
) -> None:
    run = db_sync.get(HistoryRun, history_run_id)
    if run is None:
        raise ValueError(f"HistoryRun {history_run_id} not found.")
    run.status = status
    db_sync.merge(run)
    db_sync.commit()


def update_status_of_history_unit(
    *,
    history_unit_id: int,
    status: HistoryUnitStatus,
    db_sync: Session,
) -> None:
    unit = db_sync.get(HistoryUnit, history_unit_id)
    if unit is None:
        raise ValueError(f"HistoryUnit {history_unit_id} not found.")
    unit.status = status
    db_sync.merge(unit)
    db_sync.commit()


def bulk_update_status_of_history_unit(
    *,
    history_unit_ids: list[int],
    status: HistoryUnitStatus,
    db_sync: Session,
) -> None:
    for ind in range(0, len(history_unit_ids), _CHUNK_SIZE):
        db_sync.execute(
            update(HistoryUnit)
            .where(
                HistoryUnit.id.in_(history_unit_ids[ind : ind + _CHUNK_SIZE])
            )
            .values(status=status)
        )
        # NOTE: keeping commit within the for loop is much more efficient
        db_sync.commit()


def update_logfile_of_history_unit(
    *,
    history_unit_id: int,
    logfile: str,
) -> None:
    with next(get_sync_db()) as db_sync:
        unit = db_sync.get(HistoryUnit, history_unit_id)
        if unit is None:
            raise ValueError(f"HistoryUnit {history_unit_id} not found.")
        unit.logfile = logfile
        db_sync.merge(unit)
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

    FIXME: we tried to replace `index_elements` with
    `constraint="pk_historyimagecache"`, but it did not work as expected.

    Arguments:
        list_upsert_objects:
            List of dictionaries for objects to be upsert-ed.
        db: A sync database session
    """
    if len(list_upsert_objects) == 0:
        return None

    for ind in range(0, len(list_upsert_objects), _CHUNK_SIZE):
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
