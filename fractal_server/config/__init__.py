from ._database import DatabaseSettings
from ._email import EmailSettings
from ._init_data import InitDataSettings
from ._main import Settings


def get_db_settings(db_settings=DatabaseSettings()) -> DatabaseSettings:
    return db_settings


def get_settings(settings=Settings()) -> Settings:
    return settings


def get_email_settings(email_settings=EmailSettings()) -> EmailSettings:
    return email_settings


def get_init_data_settings(
    init_data_settings=InitDataSettings(),
) -> InitDataSettings:
    return init_data_settings
