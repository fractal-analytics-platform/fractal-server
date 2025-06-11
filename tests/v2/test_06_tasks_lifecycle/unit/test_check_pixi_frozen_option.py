import pytest

from fractal_server.tasks.v2.utils_templates import _check_pixi_frozen_option


def test_check_pixi_frozen_option():
    _check_pixi_frozen_option({("A", "B")})
    with pytest.raises(ValueError):
        _check_pixi_frozen_option(
            {
                ("A", "B"),
                ("__FROZEN_OPTION__", "B"),
            }
        )
