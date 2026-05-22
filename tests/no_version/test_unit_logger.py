import logging

import pytest
import yaml

import fractal_server.logger as _logger_module
from fractal_server.logger import close_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.main import _load_logging_config


@pytest.fixture(autouse=True)
def _reset_logger_flags():
    """Restore module-level flags after every test in this file."""
    original_loaded = _logger_module._EXTERNAL_CONFIG_LOADED
    original_error = _logger_module._EXTERNAL_CONFIG_ERROR
    yield
    _logger_module._EXTERNAL_CONFIG_LOADED = original_loaded
    _logger_module._EXTERNAL_CONFIG_ERROR = original_error


def test_set_logger_is_noop_when_external_config_loaded():
    """
    When _EXTERNAL_CONFIG_LOADED is True, set_logger() must return the plain
    logger unchanged: no handlers added/removed, propagate not forced to False.
    """
    logger_name = "_test_external_noop_set_logger"
    _logger_module._EXTERNAL_CONFIG_LOADED = True

    pre_logger = logging.getLogger(logger_name)
    pre_logger.propagate = True
    pre_logger.addHandler(logging.StreamHandler())
    handlers_before = list(pre_logger.handlers)
    try:
        result = set_logger(logger_name)

        assert result is pre_logger
        assert result.propagate is True
        assert result.handlers == handlers_before
    finally:
        logging.getLogger(logger_name).handlers.clear()


def test_set_logger_warns_when_external_config_error():
    """When _EXTERNAL_CONFIG_ERROR is set, set_logger() emits a WARNING on
    first handler attachment."""
    logger_name = "_test_external_error_warning"

    _logger_module._EXTERNAL_CONFIG_LOADED = False
    _logger_module._EXTERNAL_CONFIG_ERROR = "simulated config error"

    records = []

    class _ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    logging.getLogger(logger_name).addHandler(_ListHandler())
    try:
        set_logger(logger_name)

        warning_records = [r for r in records if r.levelno == logging.WARNING]
        assert len(warning_records) >= 1
        assert "simulated config error" in warning_records[0].getMessage()
    finally:
        logging.getLogger(logger_name).handlers.clear()


def test_close_logger_is_noop_when_external_config_loaded():
    """When _EXTERNAL_CONFIG_LOADED is True, close_logger() must not
    close handlers."""
    logger_name = "_test_external_noop_close_logger"
    _logger_module._EXTERNAL_CONFIG_LOADED = True

    test_logger = logging.getLogger(logger_name)
    handler = logging.StreamHandler()
    test_logger.addHandler(handler)
    try:
        close_logger(test_logger)
        assert not handler.stream.closed
    finally:
        test_logger.handlers.clear()


def test_reset_logger_handlers_is_noop_when_external_config_loaded():
    """When _EXTERNAL_CONFIG_LOADED is True, reset_logger_handlers() must leave
    handlers in place."""
    logger_name = "_test_external_noop_reset_logger"
    _logger_module._EXTERNAL_CONFIG_LOADED = True

    test_logger = logging.getLogger(logger_name)
    test_logger.addHandler(logging.StreamHandler())
    handlers_before = list(test_logger.handlers)
    try:
        reset_logger_handlers(test_logger)
        assert test_logger.handlers == handlers_before
    finally:
        test_logger.handlers.clear()


# --- _load_logging_config tests ---


def test_load_logging_config_success(tmp_path):
    """On a valid YAML dictConfig, _EXTERNAL_CONFIG_LOADED becomes True."""
    config = {"version": 1, "disable_existing_loggers": False}
    config_file = tmp_path / "logging.yaml"
    config_file.write_text(yaml.dump(config))

    _logger_module._EXTERNAL_CONFIG_LOADED = False
    _logger_module._EXTERNAL_CONFIG_ERROR = None

    _load_logging_config(str(config_file))

    assert _logger_module._EXTERNAL_CONFIG_LOADED is True
    assert _logger_module._EXTERNAL_CONFIG_ERROR is None


def test_load_logging_config_is_noop_when_already_loaded(tmp_path):
    """When _EXTERNAL_CONFIG_LOADED is already True, a second call is
    a no-op."""
    config_file = tmp_path / "logging.yaml"
    config_file.write_text("this is not valid yaml: [")

    _logger_module._EXTERNAL_CONFIG_LOADED = True
    _logger_module._EXTERNAL_CONFIG_ERROR = None

    _load_logging_config(str(config_file))

    assert _logger_module._EXTERNAL_CONFIG_LOADED is True
    assert _logger_module._EXTERNAL_CONFIG_ERROR is None


def test_load_logging_config_file_not_found(capsys):
    """On a missing file, _EXTERNAL_CONFIG_ERROR is set and stderr
    carries a warning."""
    _logger_module._EXTERNAL_CONFIG_LOADED = False
    _logger_module._EXTERNAL_CONFIG_ERROR = None

    _load_logging_config("/nonexistent/path/logging.yaml")

    assert _logger_module._EXTERNAL_CONFIG_LOADED is False
    assert _logger_module._EXTERNAL_CONFIG_ERROR is not None
    assert "[fractal-server] WARNING" in capsys.readouterr().err


def test_load_logging_config_invalid_yaml(tmp_path, capsys):
    """On invalid YAML syntax, _EXTERNAL_CONFIG_ERROR is set and stderr
    carries a warning."""
    config_file = tmp_path / "logging.yaml"
    config_file.write_text("key: [\n  unclosed bracket\n")

    _logger_module._EXTERNAL_CONFIG_LOADED = False
    _logger_module._EXTERNAL_CONFIG_ERROR = None

    _load_logging_config(str(config_file))

    assert _logger_module._EXTERNAL_CONFIG_LOADED is False
    assert _logger_module._EXTERNAL_CONFIG_ERROR is not None
    assert "[fractal-server] WARNING" in capsys.readouterr().err


def test_load_logging_config_invalid_dictconfig(tmp_path, capsys):
    """On valid YAML but a dictConfig missing the required 'version' key,
    _EXTERNAL_CONFIG_ERROR is set and stderr carries a warning."""
    config = {"formatters": {"simple": {"format": "%(message)s"}}}
    config_file = tmp_path / "logging.yaml"
    config_file.write_text(yaml.dump(config))

    _logger_module._EXTERNAL_CONFIG_LOADED = False
    _logger_module._EXTERNAL_CONFIG_ERROR = None

    _load_logging_config(str(config_file))

    assert _logger_module._EXTERNAL_CONFIG_LOADED is False
    assert _logger_module._EXTERNAL_CONFIG_ERROR is not None
    assert "[fractal-server] WARNING" in capsys.readouterr().err
