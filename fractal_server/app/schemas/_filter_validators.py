from typing import Optional

from ._validators import valdict_keys
from fractal_server.images.models import AttributeFiltersType


def validate_type_filters(
    cls, type_filters: Optional[dict[str, bool]]
) -> dict[str, bool]:
    if type_filters is None:
        raise ValueError("'type_filters' cannot be 'None'.")

    type_filters = valdict_keys("type_filters")(cls, type_filters)
    return type_filters


def validate_attribute_filters(
    cls,
    attribute_filters: Optional[AttributeFiltersType],
) -> AttributeFiltersType:
    if attribute_filters is None:
        raise ValueError("'attribute_filters' cannot be 'None'.")

    attribute_filters = valdict_keys("attribute_filters")(
        cls, attribute_filters
    )
    for key, values in attribute_filters.items():
        if values == []:
            raise ValueError(
                f"attribute_filters[{key}] cannot be an empty list."
            )
        else:
            # values is a non-empty list, and its items must all be of the
            # same scalar non-None type
            _type = type(values[0])
            if not all(type(value) is _type for value in values):
                raise ValueError(
                    f"attribute_filters[{key}] has values with "
                    f"non-homogeneous types: {values}."
                )
            if _type not in (int, float, str, bool):
                raise ValueError(
                    f"attribute_filters[{key}] has values with "
                    f"invalid types: {values}."
                )
    return attribute_filters
