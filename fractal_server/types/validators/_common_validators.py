import os
from typing import Any

from pydantic import HttpUrl


def valdict_keys(d: dict[str, Any]) -> dict[str, Any]:
    """
    Strip every key of the dictionary, and fail if there are identical keys
    """
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
    if not os.path.isabs(path):
        raise ValueError(f"String must be an absolute path (given '{path}').")
    return path


def val_unique_list(must_be_unique: list) -> list:
    if len(set(must_be_unique)) != len(must_be_unique):
        raise ValueError("List has repetitions")
    return must_be_unique


def val_http_url(value: str) -> str:
    HttpUrl(value)
    return value
