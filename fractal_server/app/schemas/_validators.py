import os
from typing import Any
from typing import Optional
from typing import Union


def valstr(attribute: str, accept_none: bool = False):
    """
    Check that a string attribute is not an empty string, and remove the
    leading and trailing whitespace characters.

    If `accept_none`, the validator also accepts `None`.
    """

    def val(string: Optional[str]) -> Optional[str]:
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
        return s

    return val


def valdict_keys(attribute: str):
    def val(d: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        """
        Apply valstr to every key of the dictionary, and fail if there are
        identical keys.
        """
        if d is not None:
            old_keys = list(d.keys())
            new_keys = [valstr(f"{attribute}[{key}]")(key) for key in old_keys]
            if len(new_keys) != len(set(new_keys)):
                raise ValueError(
                    f"Dictionary contains multiple identical keys: {d}."
                )
            for old_key, new_key in zip(old_keys, new_keys):
                if new_key != old_key:
                    d[new_key] = d.pop(old_key)
        return d

    return val


def valdict_scalarvalues(attribute: str, accept_none: bool = True):
    """
    Check that every value of a `dict[str, list[Any]]` is a list of scalar
    values (i.e. one of int, float, str, bool or None).
    """

    def val(
        d: dict[str, list[Any]]
    ) -> dict[str, list[Union[int, float, str, bool, None]]]:
        if accept_none:
            accepted = (int, float, str, bool, type(None))
        else:
            accepted = (int, float, str, bool)
        for key, values in d.items():
            for value in values:
                if not isinstance(value, accepted):
                    raise ValueError(
                        f"{attribute}[{key}] values must be a scalars "
                        "(int, float, str, bool, or None). "
                        f"Given {value} ({type(value)})"
                    )
        return d

    return val


def valint(attribute: str, min_val: int = 1):
    """
    Check that an integer attribute (e.g. if it is meant to be the ID of a
    database entry) is greater or equal to min_val.
    """

    def val(integer: Optional[int]) -> Optional[int]:
        if integer is None:
            raise ValueError(f"Integer attribute '{attribute}' cannot be None")
        if integer < min_val:
            raise ValueError(
                f"Integer attribute '{attribute}' cannot be less than "
                f"{min_val} (given {integer})"
            )
        return integer

    return val


def val_absolute_path(attribute: str, accept_none: bool = False):
    """
    Check that a string attribute is an absolute path
    """

    def val(string: Optional[str]) -> Optional[str]:
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
    def val(must_be_unique: Optional[list]) -> Optional[list]:
        if must_be_unique is not None:
            if len(set(must_be_unique)) != len(must_be_unique):
                raise ValueError(f"`{attribute}` list has repetitions")
        return must_be_unique

    return val
