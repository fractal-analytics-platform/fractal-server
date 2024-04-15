from os.path import normpath


def normalize_url(url: str) -> str:
    if url.startswith("/"):
        return normpath(url)
    elif url.startswith("s3"):
        raise ValueError("S3 handling not implemented yet")
    else:
        raise ValueError("Generic URL handling not implemented yet")
