from typing import Annotated
from typing import Any
from typing import Union

from pydantic import AfterValidator
from pydantic.types import NonNegativeInt
from pydantic.types import StringConstraints

from fractal_server.urls import normalize_url

from .validators import val_absolute_path
from .validators import val_http_url
from .validators import val_no_dotdot_in_path
from .validators import val_non_absolute_path
from .validators import val_os_path_normpath
from .validators import val_unique_list
from .validators import valdict_keys
from .validators import validate_attribute_filters
from .validators import validate_wft_args

NonEmptyStr = Annotated[
    str,
    StringConstraints(min_length=1, strip_whitespace=True),
]
"""
A non-empty string, with no leading/trailing whitespaces.
"""


AbsolutePathStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_absolute_path),
    AfterValidator(val_no_dotdot_in_path),
    AfterValidator(val_os_path_normpath),
]
"""
String representing an absolute path.

Validation fails if the path is not absolute or if it contains a
parent-directory reference "/../".
"""

RelativePathStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_no_dotdot_in_path),
    AfterValidator(val_os_path_normpath),
    AfterValidator(val_non_absolute_path),
]

HttpUrlStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_http_url),
]
"""
String representing an URL.
"""


ZarrUrlStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_no_dotdot_in_path),
    AfterValidator(normalize_url),
]
"""
String representing a zarr URL/path.

Validation fails if the path is not absolute or if it contains a
parent-directory reference "/../".
"""


ZarrDirStr = Annotated[
    NonEmptyStr,
    AfterValidator(val_no_dotdot_in_path),
    AfterValidator(normalize_url),
]
"""
String representing a `zarr_dir` path.

Validation fails if the path is not absolute or if it contains a
parent-directory reference "/../".
"""

DictStrAny = Annotated[
    dict[str, Any],
    AfterValidator(valdict_keys),
]
"""
Dictionary where keys are strings with no leading/trailing whitespaces.
"""


DictStrStr = Annotated[
    dict[str, NonEmptyStr],
    AfterValidator(valdict_keys),
]
"""
Dictionary where keys are strings with no leading/trailing whitespaces and
values are non-empty strings.
"""

ListUniqueNonEmptyString = Annotated[
    list[NonEmptyStr],
    AfterValidator(val_unique_list),
]
"""
List of unique non-empty-string items.
"""


ListUniqueNonNegativeInt = Annotated[
    list[NonNegativeInt],
    AfterValidator(val_unique_list),
]
"""
List of unique non-negative-integer items.
"""


ListUniqueAbsolutePathStr = Annotated[
    list[AbsolutePathStr],
    AfterValidator(val_unique_list),
]
"""
List of unique absolute-path-string items.
"""

WorkflowTaskArgument = Annotated[
    DictStrAny,
    AfterValidator(validate_wft_args),
]
"""
Dictionary with no keys from a given forbid-list.
"""

ImageAttributeValue = Union[int, float, str, bool]
"""
Possible values for image attributes.
"""

ImageAttributes = Annotated[
    dict[str, ImageAttributeValue],
    AfterValidator(valdict_keys),
]
"""
Image-attributes dictionary.
"""


ImageAttributesWithNone = Annotated[
    dict[str, ImageAttributeValue | None],
    AfterValidator(valdict_keys),
]
"""
Image-attributes dictionary, including `None` attributes.
"""


AttributeFilters = Annotated[
    dict[str, list[ImageAttributeValue]],
    AfterValidator(validate_attribute_filters),
]
"""
Image-attributes filters.
"""


TypeFilters = Annotated[
    dict[str, bool],
    AfterValidator(valdict_keys),
]
"""
Image-type filters.
"""


ImageTypes = Annotated[
    dict[str, bool],
    AfterValidator(valdict_keys),
]
"""
Image types.
"""
