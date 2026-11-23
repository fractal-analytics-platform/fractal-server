import json
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Self
from uuid import UUID


class CustomEncoder(json.JSONEncoder):
    """
    Encoder which can serialize a few non-standard selected types.
    """

    def default(self: Self, obj: Any):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return obj.as_posix()
        elif isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


def json_dumps(obj: Any) -> str:
    """
    The standard `json.dumps`, with a custom encoder.

    This could be optimized through `pydantic.TypeAdapter` if needed, as in
    ```
    pydantic.TypeAdapter(Any).dump_json(obj).decode()
    ```
    See `benchmarks/json_serialization/` folder for details.
    """
    return json.dumps(obj, cls=CustomEncoder)
