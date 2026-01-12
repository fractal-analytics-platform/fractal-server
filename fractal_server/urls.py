from os.path import normpath


def normalize_url(url: str) -> str:
    url = url.strip()
    if url.startswith("/"):
        return normpath(url)
    elif url.startswith("s3"):
        return url
    else:
        raise ValueError("URLs must begin with '/' or 's3'.")
