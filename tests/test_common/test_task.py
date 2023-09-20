import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import TaskCreate
from fractal_server.common.schemas import TaskUpdate


def test_task_update():
    # Successful creation, with many unset fields
    t = TaskUpdate(name="name")
    debug(t)
    assert list(t.dict(exclude_none=True).keys()) == ["name"]
    assert list(t.dict(exclude_unset=True).keys()) == ["name"]
    # Some failures
    with pytest.raises(ValidationError):
        TaskUpdate(name="task", version="")
    TaskUpdate(name="task", version=None)
    # Successful cretion, with mutliple fields set
    t = TaskUpdate(
        name="task",
        version="1.2.3",
        owner="someone",
    )
    debug(t)
    assert t.name
    assert t.version


def test_task_create():
    # Successful creation
    t = TaskCreate(
        name="task",
        source="source",
        command="command",
        input_type="input_type",
        output_type="output_type",
        version="1.2.3",
        owner="someone",
    )
    debug(t)
    # Missing arguments
    with pytest.raises(ValidationError):
        TaskCreate(name="task", source="source")

    # Bad docs link
    with pytest.raises(ValidationError):
        TaskCreate(
            name="task",
            source="source",
            command="command",
            input_type="input_type",
            output_type="output_type",
            version="1.2.3",
            owner="someone",
            docs_link="htp://www.example.org",
        )
