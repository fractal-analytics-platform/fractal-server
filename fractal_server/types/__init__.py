from typing import Annotated
from typing import Any
from typing import Optional
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

NonEmptyString = Annotated[
    str,
    StringConstraints(min_length=1, strip_whitespace=True),
]

AbsolutePathStr = Annotated[
    NonEmptyString,
    AfterValidator(val_absolute_path),
]
HttpUrlStr = Annotated[
    NonEmptyString,
    AfterValidator(val_http_url),
]
ZarrUrlStr = Annotated[
    str,
    AfterValidator(normalize_url),
]
ZarrDirStr = Annotated[
    str,
    AfterValidator(normalize_url),
]

DictStrAny = Annotated[
    dict[str, Any],
    AfterValidator(valdict_keys),
]
DictStrStr = Annotated[
    dict[str, NonEmptyString],
    AfterValidator(valdict_keys),
]

ListUniqueNonEmptyString = Annotated[
    list[NonEmptyString],
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

ImageAttribute = Union[int, float, str, bool]
ImageAttributes = Annotated[
    dict[str, ImageAttribute],
    AfterValidator(valdict_keys),
]
ImageAttributesWithNone = Annotated[
    dict[str, Optional[ImageAttribute]],
    AfterValidator(valdict_keys),
]
AttributeFilters = Annotated[
    dict[str, list[ImageAttribute]],
    AfterValidator(validate_attribute_filters),
]
TypeFilters = Annotated[
    dict[str, bool],
    AfterValidator(valdict_keys),
]
