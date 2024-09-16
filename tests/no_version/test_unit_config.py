import sys
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.config import FractalConfigurationError
from fractal_server.config import OAuthClientConfig
from fractal_server.config import Settings
from fractal_server.syringe import Inject
from tests.fixtures_server import DB_ENGINE


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
        (
            dict(
                JWT_SECRET_KEY="secret",
                FRACTAL_TASKS_DIR="/tmp",
                DB_ENGINE="postgres-psycopg",
                POSTGRES_DB="test",
                FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
                FRACTAL_RUNNER_BACKEND="local",
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

    debug(settings_dict, raises)

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

    if settings.DB_ENGINE in ["postgres", "postgres-psycopg"] and (
        DB_ENGINE != settings.DB_ENGINE
    ):
        raises = True

    # Run Settings.check method
    if raises:
        with pytest.raises(FractalConfigurationError):
            settings.check()
    else:
        settings.check()


def test_settings_check_wrong_python():

    # Create a Settings instance
    with pytest.raises(FractalConfigurationError) as e:
        Settings(
            JWT_SECRET_KEY="secret",
            FRACTAL_TASKS_DIR="/tmp",
            FRACTAL_RUNNER_WORKING_BASE_DIR="/tmp",
            FRACTAL_RUNNER_BACKEND="local",
            SQLITE_PATH="/tmp/db.db",
            FRACTAL_TASKS_PYTHON_3_12=None,
            FRACTAL_TASKS_PYTHON_DEFAULT_VERSION="3.12",
        )
    expected_msg = (
        "FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=3.12 "
        "but FRACTAL_TASKS_PYTHON_3_12=None."
    )
    assert expected_msg in str(e.value)


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


def test_make_FRACTAL_RUNNER_WORKING_BASE_DIR_absolute():
    """
    Test `Settings.make_FRACTAL_RUNNER_WORKING_BASE_DIR_absolute` validator.
    """
    settings = Settings(
        JWT_SECRET_KEY="secret",
        SQLITE_PATH="/tmp/test.db",
        FRACTAL_RUNNER_WORKING_BASE_DIR="relative-path",
        FRACTAL_TASKS_DIR="/tmp",
    )
    debug(settings.FRACTAL_RUNNER_WORKING_BASE_DIR)
    assert settings.FRACTAL_RUNNER_WORKING_BASE_DIR.is_absolute()


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


def test_python_interpreters():
    common_attributes = dict(
        JWT_SECRET_KEY="something",
        SQLITE_PATH="/something",
        FRACTAL_RUNNER_WORKING_BASE_DIR="/something",
        FRACTAL_TASKS_DIR="/something",
    )

    # Successful branch 1: default version unset, and only one Python is set
    settings = Settings(
        FRACTAL_TASKS_PYTHON_3_9="/some/python3.9",
        FRACTAL_TASKS_PYTHON_3_10="/some/python3.10",
        FRACTAL_TASKS_PYTHON_3_11="/some/python3.11",
        FRACTAL_TASKS_PYTHON_3_12="/some/python3.12",
        **common_attributes,
    )
    version = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    assert version is not None
    version = version.replace(".", "_")
    actual_python = getattr(settings, f"FRACTAL_TASKS_PYTHON_{version}")
    assert actual_python == sys.executable
    for other_version in ["3_9", "3_10", "3_11", "3_12"]:
        if other_version != version:
            key = f"FRACTAL_TASKS_PYTHON_{other_version}"
            assert getattr(settings, key) is None

    # Successful branch 2: full configuration given
    settings = Settings(
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION="3.11",
        FRACTAL_TASKS_PYTHON_3_11="/some/python3.11",
        FRACTAL_TASKS_PYTHON_3_12="/some/python3.12",
        **common_attributes,
    )
    assert settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION is not None
    assert settings.FRACTAL_TASKS_PYTHON_3_9 is None
    assert settings.FRACTAL_TASKS_PYTHON_3_10 is None
    assert settings.FRACTAL_TASKS_PYTHON_3_11 == "/some/python3.11"
    assert settings.FRACTAL_TASKS_PYTHON_3_12 == "/some/python3.12"

    # Non-absolute paths
    with pytest.raises(FractalConfigurationError) as e:
        Settings(FRACTAL_SLURM_WORKER_PYTHON="python3.10", **common_attributes)
    assert "Non-absolute value for FRACTAL_SLURM_WORKER_PYTHON" in str(e.value)

    for version in ["3_9", "3_10", "3_11", "3_12"]:
        key = f"FRACTAL_TASKS_PYTHON_{version}"
        version_dot = version.replace("_", ".")
        attrs = common_attributes.copy()
        attrs[key] = f"python{version_dot}"
        debug(attrs)
        with pytest.raises(FractalConfigurationError) as e:
            settings = Settings(**attrs)
        assert f"Non-absolute value {key}=" in str(e.value)
