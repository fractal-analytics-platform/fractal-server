from packaging.version import parse

import fractal_server


def _check_current_version(expected_version: str):
    # Check that this module matches with the current version
    module_version = parse(expected_version)
    current_version = parse(fractal_server.__VERSION__)
    if (
        current_version.major != module_version.major
        or current_version.minor != module_version.minor
        or current_version.micro != module_version.micro
    ):
        raise RuntimeError(
            f"{fractal_server.__VERSION__=} not matching with {__file__=}"
        )
