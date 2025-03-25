import os
from typing import Annotated
from typing import Any
from typing import Optional

from pydantic.types import StringConstraints


def cant_set_none(value: Any) -> Any:
    if value is None:
        raise ValueError("Field cannot be set to 'None'.")
    return value


NonEmptyString = Annotated[
    str, StringConstraints(min_length=1, strip_whitespace=True)
]


def valdict_keys(attribute: str):
    def val(cls, d: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
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

    return val


def val_absolute_path(attribute: str, accept_none: bool = False):
    """
    Check that a string attribute is an absolute path
    """

    def val(cls, string: Optional[str]) -> Optional[str]:
        if string is None:
            if accept_none:
                return string
            else:
                raise ValueError(
                    f"String attribute '{attribute}' cannot be None"
                )
        s = string.strip()
        if not s:
            raise ValueError(f"String attribute '{attribute}' cannot be empty")
        if not os.path.isabs(s):
            raise ValueError(
                f"String attribute '{attribute}' must be an absolute path "
                f"(given '{s}')."
            )
        return s

    return val


def val_unique_list(attribute: str):
    def val(cls, must_be_unique: Optional[list]) -> Optional[list]:
        if must_be_unique is not None:
            if len(set(must_be_unique)) != len(must_be_unique):
                raise ValueError(f"`{attribute}` list has repetitions")
        return must_be_unique

    return val


def root_validate_dict_keys(cls, object: dict) -> dict:
    """
    For each dictionary in `object.values()`,
    checks that that dictionary has only keys of type str.
    """
    for dictionary in (v for v in object.values() if isinstance(v, dict)):
        if not all(isinstance(key, str) for key in dictionary.keys()):
            raise ValueError("Dictionary keys must be strings.")
    return object
