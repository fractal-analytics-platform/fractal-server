import os
from os.path import normpath
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
    s = str(path).strip()
    if not s:
        raise ValueError("String cannot be empty")
    if not os.path.isabs(s):
        raise ValueError(f"String must be an absolute path (given '{s}').")
    return s


def val_unique_list(must_be_unique: list) -> list:
    if len(set(must_be_unique)) != len(must_be_unique):
        raise ValueError("List has repetitions")
    return must_be_unique


def validate_dict_keys(obj_values: dict) -> dict:
    """
    For each dictionary in `obj_values.values()`,
    checks that that dictionary has only keys of type str.
    """
    for dictionary in (v for v in obj_values.values() if isinstance(v, dict)):
        if not all(isinstance(key, str) for key in dictionary.keys()):
            raise ValueError("Dictionary keys must be strings.")
    return obj_values


def val_http_url(value: str) -> str:
    HttpUrl(value)
    return value


def normalize_url(url: str) -> str:
    url = url.strip()
    if url.startswith("/"):
        return normpath(url)
    elif url.startswith("s3"):
        # It would be better to have a NotImplementedError
        # but Pydantic Validation + FastAPI require
        # ValueError, TypeError or AssertionError
        raise ValueError("S3 handling not implemented yet.")
    else:
        raise ValueError("URLs must begin with '/' or 's3'.")
