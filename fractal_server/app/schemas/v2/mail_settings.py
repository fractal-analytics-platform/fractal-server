from pydantic import BaseModel
from pydantic import Emailstr
from pydantic import Field


class MailSettings(BaseModel):
    sender: str
    recipients: Emailstr | None = Field(default=None)
    smtp_server: str
    port: int
    password: str
    instance_name: str
