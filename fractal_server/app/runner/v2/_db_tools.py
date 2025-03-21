from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from fractal_server.app.models.v2 import HistoryImageCache


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
    stmt = pg_insert(HistoryImageCache).values(list_upsert_objects)
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            HistoryImageCache.zarr_url,
            HistoryImageCache.dataset_id,
            HistoryImageCache.workflowtask_id,
        ],
        set_=dict(latest_history_unit_id=stmt.excluded.latest_history_unit_id),
    )
    db.execute(stmt)
    db.commit()
