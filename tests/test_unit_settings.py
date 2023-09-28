from pathlib import Path

import pytest
from devtools import debug

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
        # missing everything
        (Settings(), True),
        # missing JWT_SECRET_KEY
        (Settings(DEPLOYMENT_TYPE="testing"), True),
        # missing FRACTAL_TASKS_DIR
        (Settings(DEPLOYMENT_TYPE="testing", JWT_SECRET_KEY="secret"), True),
        # check_db
        # missing POSTGRES_DB
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="postgres",
            ),
            True,
        ),
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="postgres",
                POSTGRES_DB="fractal",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
            ),
            False,
        ),
        # missing SQLITE_PATH
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
            ),
            True,
        ),
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
            ),
            False,
        ),
        # check_runner
        # missing FRACTAL_RUNNER_WORKING_BASE_DIR
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                FRACTAL_RUNNER_BACKEND="local",
                SQLITE_PATH="/tmp/test.db",
            ),
            True,
        ),
        # missing FRACTAL_SLURM_CONFIG_FILE
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="slurm",
            ),
            True,
        ),
        # not existing FRACTAL_SLURM_CONFIG_FILE (slurm)
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="slurm",
                FRACTAL_SLURM_CONFIG_FILE="/not/existing/file.xy",
            ),
            True,
        ),
        # not existing FRACTAL_SLURM_CONFIG_FILE (local)
        (
            Settings(
                DEPLOYMENT_TYPE="testing",
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_SLURM_CONFIG_FILE="/not/existing/file.xyz",
            ),
            False,
        ),
    ],
)
def test_settings_check(settings: Settings, raises: bool):
    if raises:
        with pytest.raises(FractalConfigurationError):
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

    settings.FRACTAL_RUNNER_BACKEND = "invalid"
    debug(settings)
    with pytest.raises(FractalConfigurationError):
        settings.check()
