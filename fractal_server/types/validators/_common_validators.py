import os
import re
from pathlib import Path
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
        raise ValueError(f"Dictionary contains multiple identical keys: '{d}'.")
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


def val_s3_url(value: str) -> str:
    """
    Check that a string attribute is a valid S3 URL
    """
    # Check basic pattern
    match = re.match(r"^s3:\/\/([^\/]+)\/(.+)$", value)
    if not match:
        raise ValueError(
            f"S3 URL must match pattern 's3://bucket/key' (given '{value}')"
        )

    bucket, key = match.groups()
    ## Validate bucket name See https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    if not (3 <= len(bucket) <= 63):
        raise ValueError(
            f"S3 bucket name must be between 3 and 63 characters long "
            f"(given '{bucket}')"
        )
    if not re.match(r"^[a-z0-9][a-z0-9.\-]*[a-z0-9]$", bucket):
        raise ValueError(
            f"S3 bucket name must start and end with lowercase letter or "
            f"number, and contain only lowercase letters, numbers, periods, "
            f"and hyphens (given '{bucket}')"
        )
    if ".." in bucket:
        raise ValueError(
            f"S3 bucket name must not contain two adjacent periods "
            f"(given '{bucket}')"
        )
    if re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", bucket):
        raise ValueError(
            f"S3 bucket name must not be formatted as an IP address "
            f"(given '{bucket}')"
        )
    # Check for prohibited prefixes
    prohibited_prefixes = [
        "xn--",
        "sthree-",
        "amzn-s3-demo-",
    ]
    if any(bucket.startswith(prefix) for prefix in prohibited_prefixes):
        raise ValueError(
            f"S3 bucket name must not start with "
            f"{', '.join(repr(p) for p in prohibited_prefixes)} "
            f"(given '{bucket}')"
        )
    # Check for prohibited suffixes
    prohibited_suffixes = [
        "-s3alias",
        "--ol-s3",
        ".mrap",
        "--x-s3",
        "--table-s3",
    ]
    if any(bucket.endswith(suffix) for suffix in prohibited_suffixes):
        raise ValueError(
            f"S3 bucket name must not end with "
            f"{', '.join(repr(s) for s in prohibited_suffixes)} "
            f"(given '{bucket}')"
        )

    ## Validate key See https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    # Check key is valid UTF-8 and size
    try:
        key_bytes = key.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(
            f"S3 key must contain only valid UTF-8 characters (given '{key}')"
        )
    if len(key_bytes) > 1024:
        raise ValueError(
            f"S3 key must not exceed 1024 bytes "
            f"(given key has {len(key_bytes)} bytes)"
        )

    return value


def val_non_absolute_path(path: str) -> str:
    """
    Check that a string attribute is not an absolute path
    """
    if os.path.isabs(path):
        raise ValueError(
            f"String must not be an absolute path (given '{path}')."
        )
    return path


def val_no_dotdot_in_path(path: str) -> str:
    """
    Check that a string attribute has no '/../' in it
    """
    if ".." in Path(path).parts:
        raise ValueError("String must not contain '/../'.")
    return path


def val_os_path_normpath(path: str) -> str:
    """
    Apply `os.path.normpath` to `path`.

    Note: we keep this separate from `fractal_server.urls.normalize_url`,
    because this function only applies to on-disk paths, while `normalize_url`
    may apply to s3 URLs as well.
    """
    return os.path.normpath(path)


def val_unique_list(must_be_unique: list) -> list:
    if len(set(must_be_unique)) != len(must_be_unique):
        raise ValueError("List has repetitions")
    return must_be_unique


def val_http_url(value: str) -> str:
    HttpUrl(value)
    return value
