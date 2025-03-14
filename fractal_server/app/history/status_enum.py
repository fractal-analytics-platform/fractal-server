from enum import Enum


class XXXStatus(str, Enum):
    """
    Available status for images

    Attributes:
        SUBMITTED:
        DONE:
        FAILED:
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"
