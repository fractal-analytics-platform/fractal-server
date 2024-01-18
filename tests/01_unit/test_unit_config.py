from pathlib import Path

import pytest
from devtools import debug

from fractal_server.config import FractalConfigurationError
from fractal_server.config import OAuthClientConfig
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
        # valid
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
                SQLITE_PATH="/tmp/db.db",
            ),
            False,
        ),
        # Missing JWT_SECRET_KEY
        (
            dict(
                FRACTAL_TASKS_DIR="/tmp",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
                SQLITE_PATH="/tmp/db.db",
            ),
            True,
        ),
        # missing FRACTAL_TASKS_DIR
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
                SQLITE_PATH="/tmp/db.db",
            ),
            True,
        ),
        # check_db (sqlite)
        # Missing SQLITE_PATH
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
            ),
            True,
        ),
        # check_db (postgres)
        # missing POSTGRES_DB
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="postgres",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
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
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                FRACTAL_RUNNER_BACKEND="local",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
            ),
            False,
        ),
        # valid FRACTAL_SLURM_CONFIG_FILE variable, but missing sbatch/squeue
        # commands
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="slurm",
                FRACTAL_SLURM_CONFIG_FILE="__REPLACE_WITH_VALID_PATH__",
            ),
            True,
        ),
        # missing FRACTAL_SLURM_CONFIG_FILE variable
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
        # not existing FRACTAL_SLURM_CONFIG_FILE (with slurm backend)
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
        # not existing FRACTAL_SLURM_CONFIG_FILE (with local backend)
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
        # not existing FRACTAL_LOCAL_CONFIG_FILE
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="sqlite",
                SQLITE_PATH="/tmp/test.db",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_LOCAL_CONFIG_FILE="/not/existing/file.xyz",
            ),
            True,
        ),
    ],
)
def test_settings_check(
    settings_dict: dict[str, str], raises: bool, testdata_path: Path
):

    # Workaround to set FRACTAL_SLURM_CONFIG_FILE to a valid path, which
    # requires the value of testdata_path
    if (
        settings_dict.get("FRACTAL_SLURM_CONFIG_FILE")
        == "__REPLACE_WITH_VALID_PATH__"
    ):
        settings_dict["FRACTAL_SLURM_CONFIG_FILE"] = str(
            testdata_path / "slurm_config.json"
        )

    # Create a Settings instance
    settings = Settings(**settings_dict)

    # Run Settings.check method
    if raises:
        with pytest.raises(FractalConfigurationError):
            settings.check()
    else:
        settings.check()


def test_make_FRACTAL_TASKS_DIR_absolute():
    """
    Test `Settings.make_FRACTAL_TASKS_DIR_absolute` validator.
    """
    settings = Settings(
        JWT_SECRET_KEY="secret",
        SQLITE_PATH="/tmp/test.db",
        FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
        FRACTAL_TASKS_DIR="relative-path",
    )
    debug(settings.FRACTAL_TASKS_DIR)
    assert settings.FRACTAL_TASKS_DIR.is_absolute()


def test_OAuthClientConfig():

    config = OAuthClientConfig(
        CLIENT_NAME="GOOGLE",
        CLIENT_ID="123",
        CLIENT_SECRET="456",
    )
    debug(config)

    config = OAuthClientConfig(
        CLIENT_NAME="GITHUB",
        CLIENT_ID="123",
        CLIENT_SECRET="456",
    )
    debug(config)

    config = OAuthClientConfig(
        CLIENT_NAME="SOMETHING",
        CLIENT_ID="123",
        CLIENT_SECRET="456",
        OIDC_CONFIGURATION_ENDPOINT="endpoint",
    )
    debug(config)

    with pytest.raises(FractalConfigurationError):
        OAuthClientConfig(
            CLIENT_NAME="SOMETHING",
            CLIENT_ID="123",
            CLIENT_SECRET="456",
        )


def test_collect_oauth_clients(monkeypatch):
    settings = Settings(
        JWT_SECRET_KEY="secret",
        FRACTAL_TASKS_DIR="/tmp",
        FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
        FRACTAL_RUNNER_BACKEND="local",
        SQLITE_PATH="/tmp/db.db",
    )
    debug(settings.OAUTH_CLIENTS_CONFIG)
    assert settings.OAUTH_CLIENTS_CONFIG == []

    with monkeypatch.context() as m:
        m.setenv("OAUTH_GITHUB_CLIENT_ID", "123")
        m.setenv("OAUTH_GITHUB_CLIENT_SECRET", "456")
        settings = Settings(
            JWT_SECRET_KEY="secret",
            FRACTAL_TASKS_DIR="/tmp",
            FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
            FRACTAL_RUNNER_BACKEND="local",
            SQLITE_PATH="/tmp/db.db",
        )
        debug(settings.OAUTH_CLIENTS_CONFIG)
        assert len(settings.OAUTH_CLIENTS_CONFIG) == 1

    with monkeypatch.context() as m:
        m.setenv("OAUTH_GITHUB_CLIENT_ID", "789")
        m.setenv("OAUTH_GITHUB_CLIENT_SECRET", "012")

        m.setenv("OAUTH_MYCLIENT_CLIENT_ID", "345")
        m.setenv("OAUTH_MYCLIENT_CLIENT_SECRET", "678")
        m.setenv(
            "OAUTH_MYCLIENT_OIDC_CONFIGURATION_ENDPOINT",
            "https://example.com/.well-known/openid-configuration",
        )
        settings = Settings(
            JWT_SECRET_KEY="secret",
            FRACTAL_TASKS_DIR="/tmp",
            FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
            FRACTAL_RUNNER_BACKEND="local",
            SQLITE_PATH="/tmp/db.db",
        )
        debug(settings.OAUTH_CLIENTS_CONFIG)
        assert len(settings.OAUTH_CLIENTS_CONFIG) == 2
        names = set(c.CLIENT_NAME for c in settings.OAUTH_CLIENTS_CONFIG)
        assert names == {"GITHUB", "MYCLIENT"}
