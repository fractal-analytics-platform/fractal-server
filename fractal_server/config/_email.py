from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import Field
from pydantic import SecretStr
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from ._settings_config import SETTINGS_CONFIG_DICT


class PublicEmailSettings(BaseModel):
    """
    Schema for `EmailSettings.public`, namely the ready-to-use settings.

    Attributes:
        sender: Sender email address.
        recipients: List of recipients email address.
        smtp_server: SMTP server address.
        port: SMTP server port.
        password: Sender password.
        instance_name: Name of SMTP server instance.
        use_starttls: Whether to use the security protocol.
        use_login: Whether to use login.
    """

    sender: EmailStr
    recipients: list[EmailStr] = Field(min_length=1)
    smtp_server: str
    port: int
    password: SecretStr | None = None
    instance_name: str
    use_starttls: bool
    use_login: bool


class EmailSettings(BaseSettings):
    """
    Class with settings for email-sending feature.

    Attributes:
        FRACTAL_EMAIL_SENDER:
            Address of the OAuth-signup email sender.
        FRACTAL_EMAIL_PASSWORD:
            Password for the OAuth-signup email sender.
        FRACTAL_EMAIL_SMTP_SERVER:
            SMTP server for the OAuth-signup emails.
        FRACTAL_EMAIL_SMTP_PORT:
            SMTP server port for the OAuth-signup emails.
        FRACTAL_EMAIL_INSTANCE_NAME:
            Fractal instance name, to be included in the OAuth-signup emails.
        FRACTAL_EMAIL_RECIPIENTS:
            Comma-separated list of recipients of the OAuth-signup emails.
        FRACTAL_EMAIL_USE_STARTTLS:
            Whether to use StartTLS when using the SMTP server.
        FRACTAL_EMAIL_USE_LOGIN:
            Whether to use login when using the SMTP server.
            If 'true', FRACTAL_EMAIL_PASSWORD  must be provided.
    """

    model_config = SettingsConfigDict(**SETTINGS_CONFIG_DICT)

    FRACTAL_EMAIL_SENDER: EmailStr | None = None
    FRACTAL_EMAIL_PASSWORD: SecretStr | None = None
    FRACTAL_EMAIL_SMTP_SERVER: str | None = None
    FRACTAL_EMAIL_SMTP_PORT: int | None = None
    FRACTAL_EMAIL_INSTANCE_NAME: str | None = None
    FRACTAL_EMAIL_RECIPIENTS: str | None = None
    FRACTAL_EMAIL_USE_STARTTLS: Literal["true", "false"] = "true"
    FRACTAL_EMAIL_USE_LOGIN: Literal["true", "false"] = "true"

    public: PublicEmailSettings | None = None
    """
    The validated field which is actually used in `fractal-server`,
    automatically populated upon creation.
    """

    @model_validator(mode="after")
    def validate_email_settings(self: Self) -> Self:
        """
        Set `self.public`.
        """

        email_values = [
            self.FRACTAL_EMAIL_SENDER,
            self.FRACTAL_EMAIL_SMTP_SERVER,
            self.FRACTAL_EMAIL_SMTP_PORT,
            self.FRACTAL_EMAIL_INSTANCE_NAME,
            self.FRACTAL_EMAIL_RECIPIENTS,
        ]
        if len(set(email_values)) == 1:
            # All required EMAIL attributes are None
            pass
        elif None in email_values:
            # Not all required EMAIL attributes are set
            error_msg = (
                "Invalid FRACTAL_EMAIL configuration. "
                f"Given values: {email_values}."
            )
            raise ValueError(error_msg)
        else:
            use_starttls = self.FRACTAL_EMAIL_USE_STARTTLS == "true"
            use_login = self.FRACTAL_EMAIL_USE_LOGIN == "true"

            if use_login and self.FRACTAL_EMAIL_PASSWORD is None:
                raise ValueError(
                    "'FRACTAL_EMAIL_USE_LOGIN' is 'true' but "
                    "'FRACTAL_EMAIL_PASSWORD' is not provided."
                )

            self.public = PublicEmailSettings(
                sender=self.FRACTAL_EMAIL_SENDER,
                recipients=self.FRACTAL_EMAIL_RECIPIENTS.split(","),
                smtp_server=self.FRACTAL_EMAIL_SMTP_SERVER,
                port=self.FRACTAL_EMAIL_SMTP_PORT,
                password=self.FRACTAL_EMAIL_PASSWORD,
                instance_name=self.FRACTAL_EMAIL_INSTANCE_NAME,
                use_starttls=use_starttls,
                use_login=use_login,
            )

        return self
