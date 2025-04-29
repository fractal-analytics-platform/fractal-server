from typing import Annotated
from typing import Any
from typing import Optional

from pydantic import AfterValidator
from pydantic import BeforeValidator
from pydantic.types import StringConstraints

from ._validators import cant_set_none
from ._validators import val_unique_list
from ._validators import valdict_keys

NonEmptyString = Annotated[
    str, StringConstraints(min_length=1, strip_whitespace=True)
]

DictStrAny = Annotated[dict[str, Any], AfterValidator(valdict_keys)]
DictStrBool = Annotated[dict[str, bool], AfterValidator(valdict_keys)]

OptionalDictStrAny = Annotated[
    Optional[dict[str, Any]], AfterValidator(valdict_keys)
]

OptionalNonEmptyStringNotNone = Annotated[
    Optional[NonEmptyString], BeforeValidator(cant_set_none)
]
OptionalDictStrBoolNotNone = Annotated[
    Optional[DictStrBool], BeforeValidator(cant_set_none)
]

ListNonEmptyStringUnique = Annotated[
    list[NonEmptyString], AfterValidator(val_unique_list)
]
