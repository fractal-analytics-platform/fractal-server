import pytest
from devtools import debug

from fractal_server.config import get_settings as production_get_settings


def test_production_vs_test_defaults():

    settings = production_get_settings()
    debug(settings)

    assert settings.DB_ENGINE == "sqlite"
    assert not settings.SQLITE_PATH

    from fractal_server.config import get_settings

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
    production_settings = production_get_settings()
    from fractal_server.config import get_settings

    settings = get_settings()

    assert settings.JWT_SECRET_KEY != production_settings.JWT_SECRET_KEY
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

    override_settings_runtime(JWT_SECRET_KEY="FooBar")
    # We must `import get_settings` again
    no_import_settings = get_settings()
    assert no_import_settings.JWT_SECRET_KEY == startup_settings.JWT_SECRET_KEY

    from fractal_server.config import get_settings

    import_settings = get_settings()
    assert import_settings.JWT_SECRET_KEY != startup_settings.JWT_SECRET_KEY
    assert import_settings.JWT_SECRET_KEY == "FooBar"
