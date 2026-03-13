from os.path import join as os_path_join
from os.path import normpath
from pathlib import PurePosixPath


def normalize_url(url: str) -> str:
    """S3- aware URL normalization.
    For local paths, this is just `os.path.normpath`.
    For S3 URLs, this just strips trailing slashes. Notably, we do NOT want to
    collapse `://` into `:/`."""
    url = url.strip()
    if url.startswith("/"):
        return normpath(url)
    elif url.startswith("s3://"):
        return url.rstrip("/")
    else:
        raise ValueError("URLs must begin with '/' or 's3://'.")


def url_join(base: str, *parts: str) -> str:
    """
    S3-aware replacement for `os.path.join`.
    """
    is_s3 = base.startswith("s3://")
    # Strip scheme before joining to protect "://" from normpath
    if is_s3:
        base = base[len("s3://") :]

    # common for both local and S3 URLs
    joined = os_path_join(base, *parts)

    if is_s3:
        # Re-add scheme
        return "s3://" + joined

    return normpath(joined)


def url_is_relative_to(url: str, base: str) -> bool:
    """
    S3-aware replacement for `Path(url).is_relative_to(base)`.
    """
    url = normalize_url(url)
    base = normalize_url(base)

    url_is_s3 = url.startswith("s3://")
    base_is_s3 = base.startswith("s3://")

    if url_is_s3 != base_is_s3:
        return False  # mixed s3/local

    if url_is_s3:
        url = url[5:]
        base = base[5:]

    return PurePosixPath(url).is_relative_to(base)
