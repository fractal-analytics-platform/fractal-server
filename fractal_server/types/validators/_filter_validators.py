from typing import Any

from ._common_validators import valdict_keys


def validate_attribute_filters(
    attribute_filters: dict[str, list[int | float | str | bool]],
) -> dict[str, list[Any]]:
    attribute_filters = valdict_keys(attribute_filters)
    for key, values in attribute_filters.items():
        if values == []:
            raise ValueError(
                f"attribute_filters[{key}] cannot be an empty list."
            )
        else:
            # values is a non-empty list, and its items must homogeneous
            _type = type(values[0])
            if not all(type(value) is _type for value in values):
                raise ValueError(
                    f"attribute_filters[{key}] has values with "
                    f"non-homogeneous types: {values}."
                )
    return attribute_filters
