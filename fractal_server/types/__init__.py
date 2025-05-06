from typing import Annotated
from typing import Any

from pydantic import AfterValidator
from pydantic.types import NonNegativeInt
from pydantic.types import StringConstraints

from ..urls import normalize_url
from .validators import val_absolute_path
from .validators import val_http_url
from .validators import val_unique_list
from .validators import valdict_keys
from .validators import validate_attribute_filters
from .validators import validate_attributes
from .validators import validate_attributes_with_none
from .validators import validate_wft_args

NonEmptyString = Annotated[
    str, StringConstraints(min_length=1, strip_whitespace=True)
]

AbsolutePathStr = Annotated[NonEmptyString, AfterValidator(val_absolute_path)]
HttpUrlStr = Annotated[NonEmptyString, AfterValidator(val_http_url)]
NormalizedUrl = Annotated[str, AfterValidator(normalize_url)]

DictStrAny = Annotated[dict[str, Any], AfterValidator(valdict_keys)]
DictStrStr = Annotated[dict[str, NonEmptyString], AfterValidator(valdict_keys)]
DictStrBool = Annotated[dict[str, bool], AfterValidator(valdict_keys)]

ListNonEmptyString = Annotated[
    list[NonEmptyString], AfterValidator(val_unique_list)
]
ListNonNegativeInt = Annotated[
    list[NonNegativeInt], AfterValidator(val_unique_list)
]
ListAbsolutePathUnique = Annotated[
    list[AbsolutePathStr], AfterValidator(val_unique_list)
]

WorkflowTaskArgument = Annotated[DictStrAny, AfterValidator(validate_wft_args)]
ImageAttributes = Annotated[DictStrAny, AfterValidator(validate_attributes)]
ImageAttributesWithNone = Annotated[
    DictStrAny, AfterValidator(validate_attributes_with_none)
]
AttributeFilters = Annotated[
    dict[str, list[Any]], AfterValidator(validate_attribute_filters)
]
