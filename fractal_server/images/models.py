from pydantic import BaseModel
from pydantic import Field
from pydantic.experimental.missing_sentinel import MISSING

from fractal_server.types import DictStrAny
from fractal_server.types import ImageAttributes
from fractal_server.types import ImageAttributesWithNone
from fractal_server.types import ImageTypes
from fractal_server.types import ZarrUrlStr


class SingleImageBase(BaseModel):
    """
    Base for `SingleImage` and `SingleImageTaskOutput`.

    Attributes:
        zarr_url:
        origin:
        attributes:
        types:
    """

    zarr_url: ZarrUrlStr
    origin: ZarrUrlStr | None = None

    attributes: DictStrAny = Field(default_factory=dict)
    types: ImageTypes = Field(default_factory=dict)


class SingleImageTaskOutput(SingleImageBase):
    """
    `SingleImageBase`, with scalar `attributes` values (`None` included).
    """

    attributes: ImageAttributesWithNone = Field(default_factory=dict)


class SingleImage(SingleImageBase):
    """
    `SingleImageBase`, with scalar `attributes` values (`None` excluded)
    """

    attributes: ImageAttributes = Field(default_factory=dict)


class SingleImageUpdate(BaseModel):
    zarr_url: ZarrUrlStr
    attributes: ImageAttributes | MISSING = MISSING
    types: ImageTypes | None = None
