from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT


class InitDataSettings(BaseSettings):
    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_DEFAULT_ADMIN_EMAIL: str = "admin@fractal.xy"
    """
    Admin default email, used upon creation of the first superuser during
    server startup.

    ⚠️  **IMPORTANT**: After the server startup, you should always edit the
    default admin credentials.
    """

    FRACTAL_DEFAULT_ADMIN_PASSWORD: SecretStr = "1234"
    """
    Admin default password, used upon creation of the first superuser during
    server startup.

    ⚠️ **IMPORTANT**: After the server startup, you should always edit the
    default admin credentials.
    """
