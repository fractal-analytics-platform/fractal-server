from typing import Annotated
from typing import Any
from typing import Union

from pydantic import AfterValidator
from pydantic.types import NonNegativeInt
from pydantic.types import StringConstraints

from ..urls import normalize_url
from .validators import val_absolute_path
from .validators import val_http_url
from .validators import val_unique_list
from .validators import valdict_keys
from .validators import validate_attribute_filters
from .validators import validate_wft_args

NonEmptyStr = Annotated[
    str,
    StringConstraints(min_length=1, strip_whitespace=True),
]

AbsolutePathStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_absolute_path),
]
HttpUrlStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_http_url),
]
ZarrUrlStr = Annotated[
    NonEmptyStr,
    AfterValidator(normalize_url),
]
ZarrDirStr = Annotated[
    NonEmptyStr,
    AfterValidator(normalize_url),
]

DictStrAny = Annotated[
    dict[str, Any],
    AfterValidator(valdict_keys),
]
DictStrStr = Annotated[
    dict[str, NonEmptyStr],
    AfterValidator(valdict_keys),
]

ListUniqueNonEmptyString = Annotated[
    list[NonEmptyStr],
    AfterValidator(val_unique_list),
]
ListUniqueNonNegativeInt = Annotated[
    list[NonNegativeInt],
    AfterValidator(val_unique_list),
]
ListUniqueAbsolutePathStr = Annotated[
    list[AbsolutePathStr],
    AfterValidator(val_unique_list),
]

WorkflowTaskArgument = Annotated[
    DictStrAny,
    AfterValidator(validate_wft_args),
]

ImageAttributeValue = Union[int, float, str, bool]
ImageAttributes = Annotated[
    dict[str, ImageAttributeValue],
    AfterValidator(valdict_keys),
]
ImageAttributesWithNone = Annotated[
    dict[str, ImageAttributeValue | None],
    AfterValidator(valdict_keys),
]
AttributeFilters = Annotated[
    dict[str, list[ImageAttributeValue]],
    AfterValidator(validate_attribute_filters),
]
TypeFilters = Annotated[
    dict[str, bool],
    AfterValidator(valdict_keys),
]
ImageTypes = Annotated[
    dict[str, bool],
    AfterValidator(valdict_keys),
]
