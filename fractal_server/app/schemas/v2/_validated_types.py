from typing import Annotated

from pydantic.types import StringConstraints


NonEmptyString = Annotated[
    str, StringConstraints(min_length=1, strip_whitespace=True)
]
