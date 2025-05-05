from pathlib import Path
from typing import Annotated
from typing import Any

from pydantic import AfterValidator
from pydantic.types import NonNegativeInt
from pydantic.types import StringConstraints

from .validators import normalize_url
from .validators import val_absolute_path
from .validators import val_absolute_path_strict
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
HttpUrlStr = Annotated[str, AfterValidator(val_http_url)]

DictStrAny = Annotated[dict[str, Any], AfterValidator(valdict_keys)]
DictStrStr = Annotated[dict[str, str], AfterValidator(valdict_keys)]
DictStrBool = Annotated[dict[str, bool], AfterValidator(valdict_keys)]

ListNonEmptyStringUnique = Annotated[
    list[NonEmptyString], AfterValidator(val_unique_list)
]
ListIntUnique = Annotated[list[int], AfterValidator(val_unique_list)]
ListNonNegativeIntUnique = Annotated[
    list[NonNegativeInt], AfterValidator(val_unique_list)
]

AbsolutePath = Annotated[NonEmptyString, AfterValidator(val_absolute_path)]
ListAbsolutePathUnique = Annotated[
    list[AbsolutePath], AfterValidator(val_unique_list)
]
AbsolutePathStrict = Annotated[Path, AfterValidator(val_absolute_path_strict)]

WorkflowTaskArgument = Annotated[DictStrAny, AfterValidator(validate_wft_args)]
NormalizedUrl = Annotated[str, AfterValidator(normalize_url)]

ImageAttributes = Annotated[DictStrAny, AfterValidator(validate_attributes)]
ImageAttributesWithNone = Annotated[
    DictStrAny, AfterValidator(validate_attributes_with_none)
]

AttributeFilters = Annotated[
    dict[str, list[Any]], AfterValidator(validate_attribute_filters)
]
