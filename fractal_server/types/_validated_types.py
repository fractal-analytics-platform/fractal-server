from typing import Annotated
from typing import Any
from typing import Optional

from pydantic import AfterValidator
from pydantic.types import PositiveInt
from pydantic.types import StringConstraints

from ._filter_validators import validate_attribute_filters
from ._validators import val_absolute_path
from ._validators import val_http_url
from ._validators import val_unique_list
from ._validators import valdict_keys
from ._validators import validate_attributes
from ._validators import validate_attributes_with_none
from ._validators import validate_wft_args
from fractal_server.urls import normalize_url

NonEmptyString = Annotated[
    str, StringConstraints(min_length=1, strip_whitespace=True)
]
HttpUrl = Annotated[str, AfterValidator(val_http_url)]

DictStrAny = Annotated[dict[str, Any], AfterValidator(valdict_keys)]
DictStrStr = Annotated[dict[str, str], AfterValidator(valdict_keys)]
DictStrBool = Annotated[dict[str, bool], AfterValidator(valdict_keys)]

OptionalDictStrAny = Annotated[
    Optional[dict[str, Any]], AfterValidator(valdict_keys)
]

ListNonEmptyStringUnique = Annotated[
    list[NonEmptyString], AfterValidator(val_unique_list)
]
ListIntUnique = Annotated[list[int], AfterValidator(val_unique_list)]
ListPositiveIntUnique = Annotated[
    list[PositiveInt], AfterValidator(val_unique_list)
]

AbsolutePath = Annotated[NonEmptyString, AfterValidator(val_absolute_path)]
ListAbsolutePathUnique = Annotated[
    list[AbsolutePath], AfterValidator(val_unique_list)
]

WorkflowTaskArgument = Annotated[DictStrAny, AfterValidator(validate_wft_args)]
NormalizedUrl = Annotated[str, AfterValidator(normalize_url)]

ImageAttributes = Annotated[DictStrAny, AfterValidator(validate_attributes)]
ImageAttributesWithNone = Annotated[
    DictStrAny, AfterValidator(validate_attributes_with_none)
]

AttributeFilters = Annotated[
    dict[str, list[Any]], AfterValidator(validate_attribute_filters)
]
