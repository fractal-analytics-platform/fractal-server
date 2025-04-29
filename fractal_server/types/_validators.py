import os
from typing import Any
from typing import Optional
from typing import Union

from pydantic import HttpUrl


def cant_set_none(value: Any) -> Any:
    if value is None:
        raise ValueError("Field cannot be set to 'None'.")
    return value


def valdict_keys(d: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """
    Strip every key of the dictionary, and fail if there are identical keys
    """
    if d is not None:
        old_keys = list(d.keys())
        new_keys = [key.strip() for key in old_keys]
        if any(k == "" for k in new_keys):
            raise ValueError(f"Empty string in {new_keys}.")
        if len(new_keys) != len(set(new_keys)):
            raise ValueError(
                f"Dictionary contains multiple identical keys: '{d}'."
            )
        for old_key, new_key in zip(old_keys, new_keys):
            if new_key != old_key:
                d[new_key] = d.pop(old_key)
    return d


def val_absolute_path(path: str) -> str:
    """
    Check that a string attribute is an absolute path
    """
    s = str(path).strip()
    if not s:
        raise ValueError("String cannot be empty")
    if not os.path.isabs(s):
        raise ValueError(f"String must be an absolute path (given '{s}').")
    return s


def _val_absolute_path(accept_none: bool = False):
    """
    Check that a string attribute is an absolute path
    """

    def val(string: Optional[str]) -> Optional[str]:
        if string is None:
            if accept_none:
                return string
            else:
                raise ValueError("String cannot be None")
        s = string.strip()
        if not s:
            raise ValueError("String cannot be empty")
        if not os.path.isabs(s):
            raise ValueError(f"String must be an absolute path (given '{s}').")
        return s

    return val


def val_unique_list(must_be_unique: Optional[list]) -> Optional[list]:
    if must_be_unique is not None:
        if len(set(must_be_unique)) != len(must_be_unique):
            raise ValueError("List has repetitions")
    return must_be_unique


def root_validate_dict_keys(obj_values: dict) -> dict:
    """
    For each dictionary in `obj_values.values()`,
    checks that that dictionary has only keys of type str.
    """
    for dictionary in (v for v in obj_values.values() if isinstance(v, dict)):
        if not all(isinstance(key, str) for key in dictionary.keys()):
            raise ValueError("Dictionary keys must be strings.")
    return obj_values


def val_http_url(value: str) -> str:
    if value is not None:
        HttpUrl(value)
    return value


def validate_wft_args(value):
    if value is None:
        return
    RESERVED_ARGUMENTS = {"zarr_dir", "zarr_url", "zarr_urls", "init_args"}
    args_keys = set(value.keys())
    intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
    if intersect_keys:
        raise ValueError(
            "`args` contains the following forbidden keys: "
            f"{intersect_keys}"
        )
    return value


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
