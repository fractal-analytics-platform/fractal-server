from fastapi import HTTPException
from fastapi import status
from packaging.version import InvalidVersion
from packaging.version import Version


def _version_sort_key(version: str | None) -> tuple[int, Version | str | None]:
    """
    Returns a tuple used as (reverse) ordering key for TaskGroups in
    `get_task_group_list`.
    The parsable versions are the first in order, sorted according to the
    sorting rules of packaging.version.Version.
    Next in order we have the non-null non-parsable versions, sorted
    alphabetically.
    """
    if version is None:
        return (0, None)
    try:
        return (2, Version(version))
    except InvalidVersion:
        return (1, version)


def _find_latest_version_or_422(versions: list[str]) -> str:
    """
    > For PEP 440 versions, this is easy enough for the client to do (using
    > the `packaging` library [...]. For non-standard versions, there is no
    > well-defined ordering, and clients will need to decide on what rule is
    > appropriate for their needs.
    (https://peps.python.org/pep-0700/#why-not-provide-a-latest-version-value)

    The `versions` array is coming from the PyPI API, and its elements are
    assumed parsable.
    """
    try:
        latest = max(versions, key=lambda v_str: Version(v_str))
        return latest
    except InvalidVersion as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Cannot find latest version (original error: {str(e)}).",
        )
