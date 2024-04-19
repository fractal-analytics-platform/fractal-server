import logging
from typing import Optional

from pydantic import BaseModel


async def test_default_args_properties(task_factory_v2, caplog):
    class Foo(BaseModel):
        x: int = 42
        y: Optional[str] = None

    task = await task_factory_v2(
        name="task1",
        source="source1",
        args_schema_non_parallel=Foo.schema(),
        args_schema_parallel=Foo.schema(),
    )
    assert task.default_args_non_parallel_from_args_schema == Foo().dict(
        exclude_none=True
    )
    assert task.default_args_parallel_from_args_schema == Foo().dict(
        exclude_none=True
    )

    bugged_task = await task_factory_v2(
        name="task2",
        source="source2",
        args_schema_non_parallel=Foo().dict(),
        args_schema_parallel=Foo().dict(),
    )

    # Test KeyErrors
    caplog.set_level(logging.WARNING)
    assert caplog.text == ""

    assert bugged_task.default_args_non_parallel_from_args_schema == {}
    assert (
        "Cannot set default_args from args_schema_non_parallel" in caplog.text
    )
    assert (
        "Cannot set default_args from args_schema_parallel" not in caplog.text
    )

    assert bugged_task.default_args_parallel_from_args_schema == {}
    assert (
        "Cannot set default_args from args_schema_non_parallel" in caplog.text
    )
    assert "Cannot set default_args from args_schema_parallel" in caplog.text
