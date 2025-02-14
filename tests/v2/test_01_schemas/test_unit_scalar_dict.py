from typing import Union

import pytest
from pydantic import BaseModel
from pydantic import ValidationError


class A(BaseModel):
    x: dict[str, Union[bool, str, None]] = {}
    # NOTE the order of (bool, str, None) is important
    # even if `bool(2)==True` and `bool(None)==False`, we get "2" and None


def test_dict_A():
    assert A().x == {}

    assert A(x={"a": None}).x == {"a": None}

    assert A(x={"a": False}).x == {"a": False}

    FORBIDDEN = [
        {"d": "i", "c": "t"},
        ["l", "i", "s", "t"],
        ("t", "u", "p", "l", "e"),
        {"s", "e", "t"},
    ]

    for item in FORBIDDEN:
        with pytest.raises(ValidationError):
            A(x={"a": item})


class B(BaseModel):
    x: dict[str, str] = {}


def test_dict_B():
    assert B().x == {}

    with pytest.raises(ValidationError):
        B(x={"a": None})

    FORBIDDEN = [
        {"d": "i", "c": "t"},
        ["l", "i", "s", "t"],
        ("t", "u", "p", "l", "e"),
        {"s", "e", "t"},
    ]

    for item in FORBIDDEN:
        with pytest.raises(ValidationError):
            B(x={"a": item})
