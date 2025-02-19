from enum import Enum


class HistoryItemImageStatus(str, Enum):
    """
    Available image-status values within a `HistoryItemV2`

    Attributes:
        SUBMITTED:
        DONE:
        FAILED:
    """

    SUBMITTED = "submitted"
    DONE = "done"
    FAILED = "failed"
