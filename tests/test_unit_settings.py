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
    ("settings_dict", "raises"),
    [
        # missing JWT_SECRET_KEY
        (dict(), True),
        # missing FRACTAL_TASKS_DIR
        (dict(JWT_SECRET_KEY="secret"), True),
        # check_db
        # missing POSTGRES_DB
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="postgres",
            ),
            True,
        ),
        (
            dict(
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
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
            ),
            True,
        ),
        (
            dict(
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
            dict(
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
            dict(
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
            dict(
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
            dict(
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
def test_settings_check(settings_dict: dict[str, str], raises: bool):
    settings = Settings(**settings_dict)
    if raises:
        with pytest.raises(FractalConfigurationError):
            settings.check()
    else:
        settings.check()
