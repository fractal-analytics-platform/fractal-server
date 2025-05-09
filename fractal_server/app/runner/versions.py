import json
import sys
from typing import Union

import fractal_server


def get_versions() -> dict[str, Union[list[int], str]]:
    """
    Extract versions of Python and fractal-server.
    """
    versions = dict(
        python=tuple(sys.version_info[:3]),
        fractal_server=fractal_server.__VERSION__,
    )
    return versions


if __name__ == "__main__":
    versions = get_versions()
    print(json.dumps(versions))
