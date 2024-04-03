from copy import deepcopy
from pathlib import Path
from typing import Any


def convert_v2_args_into_v1(kwargs_v2: dict[str, Any]) -> dict[str, Any]:

    kwargs_v1 = deepcopy(kwargs_v2)

    path = kwargs_v2.pop("path")
    input_path = Path(path).parents[3].as_posix()
    component = path.replace(input_path, "").lstrip("/")

    kwargs_v1 = dict(
        input_paths=[input_path],
        output_path=input_path,
        metadata={},
        component=component,
    )

    return kwargs_v1
