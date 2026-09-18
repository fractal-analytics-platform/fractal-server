import time
from datetime import datetime

from pydantic import BaseModel
from utils_for_orm_dump import REPETITIONS
from utils_for_orm_dump import get_expected_dicts
from utils_for_orm_dump import get_orm_objects
from utils_for_orm_dump import report

from fractal_server.app.db import DB
from fractal_server.app.models.base import Base


def profile(obj: BaseModel):
    timings = []
    for _ in range(REPETITIONS):
        start = time.perf_counter()
        obj.model_dump()
        stop = time.perf_counter()
        timings.append(stop - start)
    return timings


if __name__ == "__main__":
    DB.set_sync_db()
    engine = DB.engine_sync()
    metadata = Base.metadata
    metadata.create_all(engine)

    try:
        with next(DB.get_sync_db()) as db:
            orm_objects = get_orm_objects(db)

        # Verify expected behavior
        expected_dicts = get_expected_dicts()
        for ind in range(4):
            assert orm_objects[ind].model_dump() == expected_dicts[ind]
        resource_orm_object = orm_objects[0]
        assert isinstance(resource_orm_object.timestamp_created, datetime)
        assert isinstance(
            resource_orm_object.model_dump()["timestamp_created"],
            datetime,
        )
        assert "id" not in resource_orm_object.model_dump(
            exclude={"id", "name"}
        )
        assert "id" not in resource_orm_object.model_dump(
            include={"name", "type"}
        )

        # Benchmarks
        for orm_obj in orm_objects:
            timings = profile(orm_obj)
            report(type(orm_obj).__name__, timings)

    finally:
        metadata.drop_all(engine)
        engine.dispose()
