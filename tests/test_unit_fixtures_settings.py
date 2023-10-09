import pytest
from devtools import debug

from fractal_server.config import Settings

DEFAULT_SETTINGS = Settings()


def test_production_vs_test_defaults():

    from fractal_server.config import get_settings

    debug(DEFAULT_SETTINGS)

    assert DEFAULT_SETTINGS.DB_ENGINE == "sqlite"
    assert not DEFAULT_SETTINGS.SQLITE_PATH

    settings = get_settings()
    debug(settings)

    if settings.DB_ENGINE == "sqlite":
        assert settings.SQLITE_PATH


@pytest.mark.parametrize(
    "override_settings_startup",
    [{"JWT_SECRET_KEY": "FooBar"}],
    indirect=True,
)
def test_override_settings_startup_sigle(override_settings_startup):

    from fractal_server.config import get_settings

    settings = get_settings()

    assert settings.JWT_SECRET_KEY != DEFAULT_SETTINGS.JWT_SECRET_KEY
    assert settings.JWT_SECRET_KEY == "FooBar"


TEST_SECRET_KEYS = ["Foo", "Bar"]


@pytest.mark.parametrize(
    "override_settings_startup, secret_key",
    [({"JWT_SECRET_KEY": key}, key) for key in TEST_SECRET_KEYS],
    indirect=["override_settings_startup"],
)
def test_override_settings_startup_multiple(
    override_settings_startup, secret_key
):
    from fractal_server.config import get_settings

    settings = get_settings()

    assert settings.JWT_SECRET_KEY == secret_key


def test_override_settings_runtime(override_settings_runtime):

    from fractal_server.config import get_settings

    startup_settings = get_settings()
    startup_key = startup_settings.JWT_SECRET_KEY

    new_secret_key = startup_key + "-FANCY-TAIL"
    override_settings_runtime(JWT_SECRET_KEY=new_secret_key)

    new_settings = get_settings()

    assert startup_key == "secret_key"  # from `get_default_test_settings`
    assert new_settings.JWT_SECRET_KEY == "secret_key-FANCY-TAIL"

    # NOTE: pytest first executes `override_settings_runtime`.
    # In our case, this means running the necessary code in order to build
    # `new_secret_key`.
    # After that, the test is run again from the beginning with overriden
    # settings, and that's why `startup_settings.JWT_SECRET_KEY` is
    # "secret_key-FANCY-TAIL" and not  "secret_key".
    assert startup_settings.JWT_SECRET_KEY == "secret_key-FANCY-TAIL"
