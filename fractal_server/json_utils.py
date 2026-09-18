import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return obj.as_posix()
        elif isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


def json_dumps(obj: Any) -> str:
    """
    Possible optimization through `pydantic.TypeAdapter`:
        return pydantic.TypeAdapter(Any).dump_json(obj).decode()
    See `benchmarks/bench_json_serializer.py`.
    """
    return json.dumps(obj, cls=CustomEncoder)
