from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from fractal_server.app.schemas.v2._validators_v2 import val_scalar_dict


class SingleImage(BaseModel):
    path: str
    attributes: dict[str, Any] = Field(default_factory=dict)

    _attributes = validator("attributes", allow_reuse=True)(
        val_scalar_dict("attributes")
    )

    def match_filter(self, filters: Optional[dict[str, Any]] = None):
        if filters is None:
            return True
        for key, value in filters.items():
            if value is None:
                continue
            if self.attributes.get(key) != value:
                return False
        return True
