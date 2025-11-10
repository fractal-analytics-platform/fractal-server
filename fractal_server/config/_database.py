from pydantic import SecretStr
from pydantic.types import NonNegativeInt
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from sqlalchemy.engine import URL

from ._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.types import NonEmptyStr


class DatabaseSettings(BaseSettings):
    """
    Minimal set of configurations needed for operating on the database (e.g
    for schema migrations).
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    DB_ECHO: bool = False
    """
    If `True`, make database operations verbose.
    """
    POSTGRES_USER: NonEmptyStr | None = None
    """
    User to use when connecting to the PostgreSQL database.
    """
    POSTGRES_PASSWORD: SecretStr | None = None
    """
    Password to use when connecting to the PostgreSQL database.
    """
    POSTGRES_HOST: NonEmptyStr = "localhost"
    """
    URL to the PostgreSQL server or path to a UNIX domain socket.
    """
    POSTGRES_PORT: NonNegativeInt = 5432
    """
    Port number to use when connecting to the PostgreSQL server.
    """
    POSTGRES_DB: NonEmptyStr
    """
    Name of the PostgreSQL database to connect to.
    """

    @property
    def DATABASE_URL(self) -> URL:
        if self.POSTGRES_PASSWORD is None:
            password = None
        else:
            password = self.POSTGRES_PASSWORD.get_secret_value()

        url = URL.create(
            drivername="postgresql+psycopg",
            username=self.POSTGRES_USER,
            password=password,
            host=self.POSTGRES_HOST,
            port=self.POSTGRES_PORT,
            database=self.POSTGRES_DB,
        )
        return url
