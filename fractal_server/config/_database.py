from pydantic import SecretStr
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from sqlalchemy.engine import URL

from ._settings_config import SETTINGS_CONFIG_DICT
from fractal_server.types import NonEmptyStr


class DatabaseSettings(BaseSettings):
    """
    Minimal set of configurations needed for operating on the database (e.g
    for schema migrations).

    Attributes:
        DB_ECHO:
            If `True`, make database operations verbose.
        POSTGRES_USER:
            User to use when connecting to the PostgreSQL database.
        POSTGRES_PASSWORD:
            Password to use when connecting to the PostgreSQL database.
        POSTGRES_HOST:
            URL to the PostgreSQL server or path to a UNIX domain socket.
        POSTGRES_PORT:
            Port number to use when connecting to the PostgreSQL server.
        POSTGRES_DB:
            Name of the PostgreSQL database to connect to.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    DB_ECHO: bool = False
    POSTGRES_USER: NonEmptyStr | None = None
    POSTGRES_PASSWORD: SecretStr | None = None
    POSTGRES_HOST: NonEmptyStr | None = "localhost"
    POSTGRES_PORT: NonEmptyStr | None = "5432"
    POSTGRES_DB: NonEmptyStr

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
