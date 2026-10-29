from typing import Self

from pydantic import BaseModel
from pydantic import EmailStr
from pydantic import HttpUrl
from pydantic import model_validator

from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.types import NonEmptyStr


class _URLParam(BaseModel):
    text: NonEmptyStr
    url: HttpUrl

    def to_text(self: Self) -> str:
        return f"[{self.text}]({self.url})"


class _EmailParam(BaseModel):
    text: NonEmptyStr
    email: EmailStr

    def to_text(self: Self) -> str:
        return f"[{self.text}]({self.email})"


class SelfRegistrationExceptionData(BaseModel):
    error_msg: str
    params: list[_URLParam | _EmailParam]

    @model_validator(mode="after")
    def count_params(self) -> Self:
        expected_num_params = self.error_msg.count("%s")
        num_params = len(self.params)
        if num_params != expected_num_params:
            raise ValueError(
                "Invalid message/params: "
                f"{expected_num_params=}, {num_params=}."
            )
        return self

    def to_text(self: Self) -> str:
        text = self.error_msg
        for param in self.params:
            text = text.replace(r"%s", param.to_text(), count=1)
        return text


def get_self_registration_error_data() -> SelfRegistrationExceptionData:
    """
    Construct error message to be displayed to the user.
    """
    settings = Inject(get_settings)
    registration_url = settings.FRACTAL_REGISTRATION_URL
    help_email = settings.FRACTAL_HELP_EMAIL

    if help_email is None and registration_url is None:
        error_msg = (
            "To get your Fractal account set up, please "
            "contact an administrator."
        )
        params = []
    elif registration_url is not None:
        error_msg = (
            "To get your Fractal account set up, please "
            r"follow the instructions at %s."
        )
        params = [
            {
                "text": "the registration url",
                "url": registration_url,
            }
        ]
    else:
        error_msg = (
            "To get your Fractal account set up, please "
            r"contact an admin at %s."
        )
        params = [
            {
                "text": help_email,
                "email": help_email,
            },
        ]
    data = SelfRegistrationExceptionData(
        error_msg=error_msg,
        params=params,
    )
    return data
