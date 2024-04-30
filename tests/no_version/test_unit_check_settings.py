import pytest

from fractal_server.main import check_settings


@pytest.mark.parametrize("logging_level", [0, 10, 20, 30])
def test_check_settings(logging_level, override_settings_factory):
    """
    This test simply runs `check_settings` with several logging
    levels, but it does not assert anything (because we could not
    find how to capture custom loggers through the `caplog` fixture).
    """
    override_settings_factory(FRACTAL_LOGGING_LEVEL=logging_level)
    check_settings()
