import json
import sys
from typing import TypedDict

import fractal_server


class VersionsType(TypedDict):
    python: tuple[int, ...]
    fractal_server: str


def get_versions() -> VersionsType:
    """
    Extract versions of Python and fractal-server.
    """
    return dict(
        python=tuple(sys.version_info[:3]),
        fractal_server=fractal_server.__VERSION__,
    )


if __name__ == "__main__":
    versions = get_versions()
    print(json.dumps(versions))
