from os.path import normpath


def normalize_url(url: str, allow_blanks: bool = False) -> str:
    url = url.strip()
    if not allow_blanks and " " in url:
        raise ValueError("White spaces are not allowed in URLs.")
    if url.startswith("/"):
        return normpath(url)
    elif url.startswith("s3"):
        # It would be better to have a NotImplementedError
        # but Pydantic Validation + FastAPI require
        # ValueError, TypeError or AssertionError
        raise ValueError("S3 handling not implemented yet.")
    else:
        raise ValueError("URLs must begin with '/' or 's3'.")
