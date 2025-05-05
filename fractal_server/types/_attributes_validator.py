from typing import Any
from typing import Union


def validate_attributes(
    v: dict[str, Any]
) -> dict[str, Union[int, float, str, bool]]:
    for key, value in v.items():
        if not isinstance(value, (int, float, str, bool)):
            raise ValueError(
                f"attributes[{key}] must be a scalar "
                f"(int, float, str or bool). Given {value} ({type(value)})"
            )
    return v


def validate_attributes_with_none(
    v: dict[str, Any]
) -> dict[str, Union[int, float, str, bool, None]]:
    for key, value in v.items():
        if not isinstance(value, (int, float, str, bool, type(None))):
            raise ValueError(
                f"attributes[{key}] must be a scalar (int, float, str, bool)"
                f" or None. Given {value} ({type(value)})"
            )
    return v
