from typing import Any

from fractal_server.app.models import DatasetV2
from fractal_server.app.models import dump_model


async def _get_dataset_attrs(db, dataset_id) -> dict[str, Any]:
    await db.close()
    db_dataset = await db.get(DatasetV2, dataset_id)
    dataset_attrs = dump_model(db_dataset, include={"images"})
    return dataset_attrs
