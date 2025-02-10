from datetime import datetime

from pydantic import BaseModel


class AccountingRead(BaseModel):

    id: int
    user_id: int
    timestamp: datetime
    num_tasks: int
    num_new_images: int
