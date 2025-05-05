from typing import Optional

from pydantic import BaseModel
from pydantic import Field

from fractal_server.types import DictStrAny
from fractal_server.types import DictStrBool
from fractal_server.types import ImageAttributes
from fractal_server.types import ImageAttributesWithNone
from fractal_server.types import NormalizedUrl


class _SingleImageBase(BaseModel):
    """
    Base for SingleImage and SingleImageTaskOutput.

    Attributes:
        zarr_url:
        origin:
        attributes:
        types:
    """

    zarr_url: NormalizedUrl
    origin: Optional[NormalizedUrl] = None

    attributes: DictStrAny = Field(default_factory=dict)
    types: DictStrBool = Field(default_factory=dict)


class SingleImageTaskOutput(_SingleImageBase):
    """
    `SingleImageBase`, with scalar `attributes` values (`None` included).
    """

    attributes: ImageAttributesWithNone = Field(default_factory=dict)


class SingleImage(_SingleImageBase):
    """
    `SingleImageBase`, with scalar `attributes` values (`None` excluded).
    """

    attributes: ImageAttributes = Field(default_factory=dict)


class SingleImageUpdate(BaseModel):
    zarr_url: NormalizedUrl
    attributes: ImageAttributes = None
    types: Optional[DictStrBool] = None
