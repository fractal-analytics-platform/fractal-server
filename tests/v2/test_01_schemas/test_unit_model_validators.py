import pytest

from fractal_server.types.validators import validate_dict_keys


def test_unit_validate_dict_keys():

    # OK
    obj_values = {
        0: {"key1": 0, "key2": 0},
        1: {"key3": 1, "key4": 1},
    }
    assert validate_dict_keys(obj_values) == obj_values

    # Fail
    obj_values = {
        0: {"key1": 0, "key2": 0},
        1: {"key3": 1, 4: 1},
    }
    with pytest.raises(ValueError, match="Dictionary keys must be strings"):
        validate_dict_keys(obj_values)
