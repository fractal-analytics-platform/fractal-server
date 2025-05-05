import pytest

from fractal_server.types.validators import root_validate_dict_keys


def test_unit_root_validate_dict_keys():

    # OK
    obj_values = {
        0: {"key1": 0, "key2": 0},
        1: {"key3": 1, "key4": 1},
    }
    assert root_validate_dict_keys(obj_values) == obj_values

    # Fail
    obj_values = {
        0: {"key1": 0, "key2": 0},
        1: {"key3": 1, 4: 1},
    }
    with pytest.raises(ValueError) as e:
        root_validate_dict_keys(obj_values)
    assert "Dictionary keys must be strings" in e._excinfo[1].args[0]
