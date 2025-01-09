import os
from typing import Any
from typing import Optional


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
                    f"Dictionary contains multiple identical keys: '{d}'."
                )
            for old_key, new_key in zip(old_keys, new_keys):
                if new_key != old_key:
                    d[new_key] = d.pop(old_key)
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


def validate_type_filters(
    type_filters: Optional[dict],
) -> Optional[dict[str, bool]]:
    if type_filters is not None:
        type_filters = valdict_keys("type_filters")(type_filters)
    return type_filters


def validate_attribute_filters(
    attribute_filters: Optional[dict[str, list[Any]]]
) -> Optional[dict[str, list[Any]]]:
    if attribute_filters is not None:
        attribute_filters = valdict_keys("attribute_filters")(
            attribute_filters
        )
        for key, values in attribute_filters.items():
            if values:
                _type = type(values[0])
                if not all(isinstance(value, _type) for value in values):
                    raise ValueError(
                        f"attribute_filters[{key}] has values with "
                        f"non-homogeneous types: {values}."
                    )
                if _type not in (int, float, str, bool, type(None)):
                    # FIXME: Review whether None is accepted
                    raise ValueError(
                        f"attribute_filters[{key}] has values with "
                        f"invalid types: {values}."
                    )
    return attribute_filters


def root_validate_dict_keys(cls, object: dict) -> dict:
    for dictionary in (v for v in object.values() if isinstance(v, dict)):
        if not all(isinstance(key, str) for key in dictionary.keys()):
            raise ValueError("Dictionary keys must be strings.")
    return object
