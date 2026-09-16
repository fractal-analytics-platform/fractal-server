import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from devtools import debug
from pydantic_core import PydanticSerializationError
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import synonym

from fractal_server.app.models import dump_model
from fractal_server.app.models import dump_model_to_json
from fractal_server.app.models.base import Base
from fractal_server.app.models.v2.history import HistoryRun
from fractal_server.app.models.v2.history import HistoryUnit


def _history_run(**overrides) -> HistoryRun:
    kwargs = dict(
        dataset_id=1,
        job_id=1,
        task_id=1,
        workflowtask_dump={"a": 1},
        task_group_dump={"b": 2},
        timestamp_started=datetime(2023, 1, 1, 12, 0, 0),
        status="done",
        num_available_images=5,
    )
    kwargs.update(overrides)
    return HistoryRun(**kwargs)


def test_dump_model_scalar_types():
    """
    `dump_model` returns plain Python values (no serialization yet).
    """
    hr = _history_run()
    dumped = dump_model(hr)
    assert dumped["status"] == "done"
    assert dumped["num_available_images"] == 5
    assert dumped["timestamp_started"] == datetime(2023, 1, 1, 12, 0, 0)


def test_dump_model_to_json_datetime_is_isoformat():
    hr = _history_run()
    dumped = json.loads(dump_model_to_json(hr))
    assert (
        dumped["timestamp_started"]
        == datetime(2023, 1, 1, 12, 0, 0).isoformat()
    )


def test_dump_model_to_json_json_column_round_trips():
    """
    Values that are already JSON-native (dict/list/str/int/float/bool/None)
    are left untouched, however deeply nested.
    """
    hr = _history_run(
        workflowtask_dump={"nested": {"list": [1, 2, {"x": None}]}},
    )
    dumped = json.loads(dump_model_to_json(hr))
    assert dumped["workflowtask_dump"] == {
        "nested": {"list": [1, 2, {"x": None}]}
    }


def test_dump_model_to_json_array_column():
    hu = HistoryUnit(
        history_run_id=1,
        logfile="x.log",
        status="done",
        zarr_urls=["/a/b", "/c/d"],
    )
    dumped = json.loads(dump_model_to_json(hu))
    assert dumped["zarr_urls"] == ["/a/b", "/c/d"]


async def test_dump_model_to_json_array_of_timestamps_nested_in_json_column(
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    workflowtask_factory,
    job_factory,
    db_sync,
    MockCurrentUser,
):
    """
    There is no ARRAY(DateTime) column in the schema today, but a JSON
    column can hold a list of `datetime` objects on the Python side (e.g.
    before it round-trips through the database). Each element is
    serialized independently and recursively, the same way a top-level
    `datetime` column is: as an ISO-8601 string.
    """
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project_id=project.id)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id)
        await workflowtask_factory(workflow_id=workflow.id, task_id=task.id)
        job = await job_factory(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir="/foo",
            status="done",
        )

    hr = _history_run(
        dataset_id=dataset.id,
        job_id=job.id,
        task_id=task.id,
        workflowtask_dump={
            "timestamps": [
                datetime(2023, 1, 1),
                datetime(2023, 1, 2, 8, 30),
            ]
        },
    )

    db_sync.add(hr)
    db_sync.commit()
    db_sync.refresh(hr)
    debug(hr)

    dumped = json.loads(dump_model_to_json(hr))
    assert dumped["workflowtask_dump"]["timestamps"] == [
        datetime(2023, 1, 1, 0, 0).isoformat(),
        datetime(2023, 1, 2, 8, 30).isoformat(),
    ]


def test_dump_model_to_json_uuid_and_path_in_json_column():
    """
    `UUID`/`Path` values are not used by any current column, but if one
    ever ends up inside a `dict[str, Any]`/JSON column, it is serialized
    to its string representation - same as pydantic's `model_dump_json`.
    """
    some_uuid = uuid.uuid4()
    hr = _history_run(
        workflowtask_dump={"id": some_uuid, "path": Path("/tmp/foo/bar")},
    )
    dumped = json.loads(dump_model_to_json(hr))
    assert dumped["workflowtask_dump"] == {
        "id": str(some_uuid),
        "path": "/tmp/foo/bar",
    }


def test_dump_model_to_json_unsupported_type_raises():
    """
    An arbitrary object with no known serializer is *not* silently
    stringified: it raises, so unsupported data shapes fail loudly
    instead of producing a subtly wrong dump.
    """

    class Unsupported:
        pass

    hr = _history_run(workflowtask_dump={"x": Unsupported()})
    with pytest.raises(PydanticSerializationError):
        dump_model_to_json(hr)


def test_dump_model_to_json_include_exclude():
    hr = _history_run()
    only_status = json.loads(dump_model_to_json(hr, include={"status"}))
    assert only_status == {"status": "done"}

    without_status = json.loads(dump_model_to_json(hr, exclude={"status"}))
    assert "status" not in without_status
    assert without_status["num_available_images"] == 5


class _TableWithAlias(Base):
    __tablename__ = "test_table_with_alias"

    id: Mapped[int] = mapped_column(primary_key=True)
    attribute: Mapped[str] = mapped_column("db_col")
    attr_alias = synonym("attribute")


def test_dump_model_supports_column_name_and_synonym_aliases():
    obj = _TableWithAlias(id=1, attr_alias="hello")
    assert obj.attribute == obj.attr_alias == "hello"

    dumped = dump_model(obj)
    assert dumped == {"id": 1, "attribute": "hello"}

    dumped_json = json.loads(dump_model_to_json(obj))
    assert dumped_json == {"id": 1, "attribute": "hello"}
