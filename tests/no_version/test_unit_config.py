from pathlib import Path

import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.routes.auth.oauth import get_oauth_router
from fractal_server.config import DatabaseSettings
from fractal_server.config import DataSettings
from fractal_server.config import EmailSettings
from fractal_server.tasks.config import PixiSLURMConfig
from fractal_server.tasks.config import TasksPixiSettings
from fractal_server.tasks.config import TasksPythonSettings


@pytest.mark.parametrize(
    ("settings_dict", "raises"),
    [
        # valid FRACTAL_DATA_BASE_FOLDER
        # (FRACTAL_DATA_AUTH_SCHEME="users-folders")
        (
            dict(
                FRACTAL_DATA_AUTH_SCHEME="users-folders",
                FRACTAL_DATA_BASE_FOLDER="/path/to/base",
            ),
            False,
        ),
        # missing FRACTAL_DATA_BASE_FOLDER
        # (FRACTAL_DATA_AUTH_SCHEME="users-folders")
        (
            dict(
                FRACTAL_DATA_AUTH_SCHEME="users-folders",
            ),
            True,
        ),
        # not absolute FRACTAL_DATA_BASE_FOLDER
        # (FRACTAL_DATA_AUTH_SCHEME="users-folders")
        (
            dict(
                FRACTAL_DATA_AUTH_SCHEME="users-folders",
                FRACTAL_DATA_BASE_FOLDER="invalid/relative/path",
            ),
            True,
        ),
    ],
)
def test_data_settings(
    settings_dict: dict[str, str],
    raises: bool,
    testdata_path: Path,
):
    debug(settings_dict, raises)

    if raises:
        with pytest.raises(ValueError):
            DataSettings(**settings_dict)
    else:
        DataSettings(**settings_dict)


def test_database_settings():
    """
    Note: this relies on `pytest-env` configuration in `pyproject.toml`.
    """
    ds = DatabaseSettings()
    assert (
        str(ds.DATABASE_URL)
        == "postgresql+psycopg://postgres:***@localhost:5432/fractal_test"
    )
    ds = DatabaseSettings(POSTGRES_PASSWORD=None)
    assert (
        str(ds.DATABASE_URL)
        == "postgresql+psycopg://postgres@localhost:5432/fractal_test"
    )


def test_OAuthSettings():
    from fractal_server.config import OAuthSettings

    config = OAuthSettings(
        OAUTH_CLIENT_NAME="GOOGLE",
        OAUTH_CLIENT_ID="123",
        OAUTH_CLIENT_SECRET="456",
    )
    assert config.is_set
    assert config.OAUTH_CLIENT_NAME == "google"

    config = OAuthSettings(
        OAUTH_CLIENT_NAME="GITHUB",
        OAUTH_CLIENT_ID="123",
        OAUTH_CLIENT_SECRET="456",
    )
    assert config.is_set
    assert config.OAUTH_CLIENT_NAME == "github"

    config = OAuthSettings(
        OAUTH_CLIENT_NAME="SOMETHING",
        OAUTH_CLIENT_ID="123",
        OAUTH_CLIENT_SECRET="456",
        OAUTH_OIDC_CONFIG_ENDPOINT="endpoint",
    )
    assert config.is_set

    config = OAuthSettings(
        OAUTH_CLIENT_ID="123",
        OAUTH_CLIENT_SECRET="456",
    )
    assert not config.is_set

    with pytest.raises(ValueError):
        OAuthSettings(
            OAUTH_CLIENT_NAME="SOMETHING",
            OAUTH_CLIENT_ID="123",
            OAUTH_CLIENT_SECRET="456",
        )


def test_get_oauth_router(override_oauth_settings_factory):
    override_oauth_settings_factory(
        OAUTH_CLIENT_NAME="github",
        OAUTH_CLIENT_ID="id",
        OAUTH_CLIENT_SECRET="secret",
    )
    assert get_oauth_router() is not None

    override_oauth_settings_factory(
        OAUTH_CLIENT_NAME="google",
        OAUTH_CLIENT_ID="id",
        OAUTH_CLIENT_SECRET="secret",
    )
    assert get_oauth_router() is not None

    url = "https://accounts.google.com/.well-known/openid-configuration"
    override_oauth_settings_factory(
        OAUTH_CLIENT_NAME="example-oidc",
        OAUTH_CLIENT_ID="id",
        OAUTH_CLIENT_SECRET="secret",
        OAUTH_OIDC_CONFIG_ENDPOINT=url,
    )
    assert get_oauth_router() is not None


def test_email_settings():
    required_mail_args = dict(
        FRACTAL_EMAIL_SENDER="sender@example.org",
        FRACTAL_EMAIL_SMTP_SERVER="smtp_server",
        FRACTAL_EMAIL_SMTP_PORT=54321,
        FRACTAL_EMAIL_INSTANCE_NAME="test",
        FRACTAL_EMAIL_RECIPIENTS="a@fracta.xy,b@fractal.yx",
    )
    # 1: no mail settings
    email_settings = EmailSettings()
    assert email_settings.public is None
    # 2: FRACTAL_EMAIL_USE_LOGIN is true, but no password settings
    with pytest.raises(ValidationError):
        EmailSettings(
            **required_mail_args,
        )
    # 3: missing password
    with pytest.raises(ValidationError):
        EmailSettings(**required_mail_args)
    # 4: ok
    email_settings = EmailSettings(
        **required_mail_args,
        FRACTAL_EMAIL_PASSWORD="password",
    )
    assert email_settings.public is not None
    assert len(email_settings.public.recipients) == 2
    # 5: FRACTAL_EMAIL_USE_LOGIN is false and no password needed
    email_settings = EmailSettings(
        **required_mail_args,
        FRACTAL_EMAIL_USE_LOGIN="false",
    )
    assert email_settings.public is not None
    # 6: missing required arguments
    for arg in required_mail_args:
        with pytest.raises(
            ValidationError,
            match="Invalid FRACTAL_EMAIL configuration",
        ):
            EmailSettings(
                **{k: v for k, v in required_mail_args.items() if k != arg},
                FRACTAL_EMAIL_USE_LOGIN="false",
            )
    # 8: fail with sender emails
    with pytest.raises(ValidationError):
        EmailSettings(
            **{
                k: v
                for k, v in required_mail_args.items()
                if k != "FRACTAL_EMAIL_SENDER"
            },
            FRACTAL_EMAIL_SENDER="not-an-email",
            FRACTAL_EMAIL_USE_LOGIN="false",
        )
    with pytest.raises(ValidationError):
        EmailSettings(
            **{
                k: v
                for k, v in required_mail_args.items()
                if k != "FRACTAL_EMAIL_RECIPIENTS"
            },
            FRACTAL_EMAIL_RECIPIENTS="not,emails",
            FRACTAL_EMAIL_USE_LOGIN="false",
        )


def test_python_config():
    valid = dict(default_version="3.10", versions={"3.10": "/fake"})
    TasksPythonSettings(**valid)

    invalid = dict(default_version="3.11", versions={"3.10": "/fake"})
    with pytest.raises(ValueError):
        TasksPythonSettings(**invalid)


def test_pixi_config():
    # Valid Pixi config
    pixi_config = {
        "default_version": "0.41.0",
        "versions": {
            "0.40.0": "/common/path/pixi/0.40.0/",
            "0.41.0": "/common/path/pixi/0.41.0/",
            "0.43.0": "/common/path/pixi/0.43.0/",
        },
        "TOKIO_WORKER_THREADS": 2,
        "PIXI_CONCURRENT_SOLVES": 4,
        "PIXI_CONCURRENT_DOWNLOADS": 4,
        "DEFAULT_ENVIRONMENT": "default",
        "DEFAULT_PLATFORM": "linux-64",
        "SLURM_CONFIG": None,
    }

    TasksPixiSettings(**pixi_config)

    # Invalid Pixi config 1
    pixi_config = {
        "default_version": "1.2.3",
        "versions": {
            "0.40.0": "/common/path/pixi/0.40.0/",
            "0.41.0": "/common/path/pixi/0.41.0/",
            "0.43.0": "/common/path/pixi/0.43.0/",
        },
    }

    with pytest.raises(ValidationError):
        TasksPixiSettings(**pixi_config)

    # Invalid Pixi config 2
    pixi_config = {
        "default_version": "0.41.0",
        "versions": {
            "0.40.0": "/common/path/pixi/0.40.0/",
            "0.41.0": "/common/path/pixi/0.41.0/",
            "0.43.0": "/different/path/pixi/0.43.0/",
        },
    }
    with pytest.raises(ValidationError):
        TasksPixiSettings(**pixi_config)

    # Invalid Pixi config 3
    pixi_config = {
        "default_version": "0.41.0",
        "versions": {
            "0.40.0": "/common/path/pixi/0.40.0/",
            "0.41.0": "/common/path/pixi/0.41.0/",
            "0.43.0": "/common/path/pixi/0.43.1/",
        },
    }
    with pytest.raises(ValidationError):
        TasksPixiSettings(**pixi_config)


def test_pixi_slurm_config():
    PixiSLURMConfig(
        partition="fake",
        time="100",
        cpus=1,
        mem="10K",
    )
    with pytest.raises(
        ValueError,
        match="units suffix",
    ):
        PixiSLURMConfig(
            partition="fake",
            time="100",
            cpus=1,
            mem="1000",
        )
    PixiSLURMConfig(
        partition="fake",
        time="100",
        cpus=1,
        mem="1000M",
    )
    PixiSLURMConfig(
        partition="fake",
        time="100",
        cpus=1,
        mem="10G",
    )
