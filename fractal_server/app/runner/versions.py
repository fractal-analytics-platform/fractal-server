import json
import sys
from typing import Union

import cloudpickle

import fractal_server


def get_versions() -> dict[str, Union[list[int], str]]:
    """
    Extract versions of Python, fractal-server and cloudpickle.

    This information is useful to check compatibility of two Python
    interpreters when running tasks: the current interpreter (which prepares
    the input pickles and orchestrates workflow execution) and a remote
    interpreter (e.g. the one defined in the `FRACTAL_SLURM_WORKER_PYTHON`
    configuration variable) that executes the tasks.
    """
    versions = dict(
        python=list(sys.version_info[:3]),
        cloudpickle=cloudpickle.__version__,
        fractal_server=fractal_server.__VERSION__,
    )
    return versions


if __name__ == "__main__":
    versions = get_versions()
    print(json.dumps(versions))
