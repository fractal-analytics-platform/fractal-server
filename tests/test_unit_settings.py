from pathlib import Path

import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.config import FractalConfigurationError
from fractal_server.config import Settings
from fractal_server.syringe import Inject


def test_settings_injection(override_settings):
    """
    GIVEN an Inject object with a Settigns object registered to it
    WHEN I ask for the Settings object
    THEN it gets returned
    """
    settings = Inject(Settings)
    debug(settings)
    assert isinstance(settings, Settings)


@pytest.mark.parametrize(
    ("settings", "raises"),
    [
        (Settings(), True),
        (
            Settings(
                DEPLOYMENT_TYPE="development",
                JWT_SECRET_KEY="secret",
                SQLITE_PATH="path",
                FRACTAL_TASKS_DIR=Path("/tmp"),
                FRACTAL_RUNNER_WORKING_BASE_DIR=Path("/tmp"),
            ),
            False,
        ),
    ],
)
def test_settings_check(settings: Settings, raises: bool):
    debug(settings)
    if raises:
        with pytest.raises(ValidationError):
            settings.check()
    else:
        settings.check()


def test_FractalConfigurationError():
    """
    Stub test to verify some expected behaviors of Settings.check() method
    """

    settings = Settings(
        DEPLOYMENT_TYPE="development",
        JWT_SECRET_KEY="secret",
        SQLITE_PATH="path",
        FRACTAL_TASKS_DIR=Path("/tmp"),
        FRACTAL_RUNNER_WORKING_BASE_DIR=Path("/tmp"),
    )
    debug(settings)
    settings.check()

    settings.FRACTAL_LOCAL_RUNNER_MAX_TASKS_PER_WORKFLOW = None
    debug(settings)
    settings.check()

    settings.FRACTAL_LOCAL_RUNNER_MAX_TASKS_PER_WORKFLOW = 1
    debug(settings)
    settings.check()

    settings.FRACTAL_LOCAL_RUNNER_MAX_TASKS_PER_WORKFLOW = 0
    debug(settings)
    with pytest.raises(FractalConfigurationError):
        settings.check()

    settings.FRACTAL_LOCAL_RUNNER_MAX_TASKS_PER_WORKFLOW = -123
    debug(settings)
    with pytest.raises(FractalConfigurationError):
        settings.check()
