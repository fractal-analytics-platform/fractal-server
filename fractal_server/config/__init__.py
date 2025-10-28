from ._data import DataAuthScheme  # noqa F401
from ._data import DataSettings
from ._database import DatabaseSettings
from ._email import EmailSettings
from ._email import PublicEmailSettings  # noqa F401
from ._main import Settings
from ._oauth import OAuthSettings


def get_db_settings(db_settings=DatabaseSettings()) -> DatabaseSettings:
    return db_settings


def get_settings(settings=Settings()) -> Settings:
    return settings


def get_email_settings(email_settings=EmailSettings()) -> EmailSettings:
    return email_settings


def get_oauth_settings(oauth_settings=OAuthSettings()) -> OAuthSettings:
    return oauth_settings


def get_data_settings(data_settings=DataSettings()) -> DataSettings:
    return data_settings
