import pytest
from devtools import debug
from pydantic import BaseModel
from pydantic import Field

from fractal_server.types import AttributeFilters

VALID_ATTRIBUTE_FILTERS = (
    {},
    {"key1": ["A"]},
    {"key1": ["A", "B"]},
    {"key1": [1, 2]},
    {"key1": [True, False]},
    {"key1": [1.5, -1.2]},
    {"key1": [1, 2], "key2": ["A", "B"]},
)

INVALID_ATTRIBUTE_FILTERS = (
    {"key1": None},
    {"key1": []},
    {True: ["value"]},  # non-string key
    {1: ["value"]},  # non-string key
    {"key1": 1},  # not a list
    {"key1": True},  # not a list
    {"key1": "something"},  # not a list
    {"key1": [1], " key1": [1]},  # non-unique normalized keys
    {"key1": [None]},  # None value
    {"key1": [1, 1.0]},  # non-homogeneous types
    {"key1": [1, "a"]},  # non-homogeneous types
    {"key1": [[1, 2], [1, 2]]},  # non-scalar type
    {"key1": [1, True]},  # non-homogeneous types
)


class MyModel(BaseModel):
    attribute_filters: AttributeFilters = Field(default_factory=dict)


@pytest.mark.parametrize("attribute_filters", VALID_ATTRIBUTE_FILTERS)
def test_valid_attribute_filters(attribute_filters: dict):
    debug(attribute_filters)
    MyModel(attribute_filters=attribute_filters)


@pytest.mark.parametrize("attribute_filters", INVALID_ATTRIBUTE_FILTERS)
def test_invalid_attribute_filters(attribute_filters: dict):
    debug(attribute_filters)
    with pytest.raises(ValueError) as e:
        MyModel(attribute_filters=attribute_filters)
    debug(e.value)
