# This adapts clusterfutures <https://github.com/sampsyo/clusterfutures>
# Original Copyright
# Copyright 2021 Adrian Sampson <asampson@cs.washington.edu>
# License: MIT
#
# Modified by:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
"""
This module provides a simple self-standing script that executes arbitrary
python code received via pickled files on a cluster node.
"""
import argparse
import logging
import os
import sys
from typing import Literal
from typing import Optional
from typing import Type
from typing import Union

import cloudpickle

from fractal_server import __VERSION__


class ExceptionProxy:
    """
    Proxy class to serialise exceptions

    In general exceptions are not serialisable. This proxy class saves the
    serialisable content of an exception. On the receiving end, it can be used
    to reconstruct a TaskExecutionError.

    Attributes:
        exc_type_name: Name of the exception type
        tb: TBD
        args: TBD
        kwargs: TBD
    """

    def __init__(
        self, exc_type: Type[BaseException], tb: str, *args, **kwargs
    ):
        self.exc_type_name: str = exc_type.__name__
        self.tb: str = tb
        self.args = args
        self.kwargs: dict = kwargs


class FractalVersionMismatch(RuntimeError):
    """
    Custom exception for version mismatch
    """

    pass


def _check_versions_mismatch(
    server_versions: dict[
        Literal["python", "fractal_server", "cloudpickle"],
        Union[str, tuple[int]],
    ]
):
    """
    Compare the server {python,cloudpickle,fractal_server} versions with the
    ones available to the current worker

    Arguments:
        server_versions:
            The version used in the fractal-server instance that created the
            cloudpickle file

    Raises:
        FractalVersionMismatch: If the cloudpickle or fractal_server versions
                                do not match with the ones on the server
    """

    server_python_version = server_versions["python"]
    worker_python_version = sys.version_info[:3]
    if worker_python_version != server_python_version:
        # FIXME: turn this into an error, after fixing a broader CI issue, see
        # https://github.com/fractal-analytics-platform/fractal-server/issues/375
        logging.critical(
            f"{server_python_version=} but {worker_python_version=}. "
            "cloudpickle is not guaranteed to correctly load "
            "pickle files created with different python versions. "
            "Note, however, that if you reached this line it means that "
            "the pickle file was likely loaded correctly."
        )

    server_cloudpickle_version = server_versions["cloudpickle"]
    worker_cloudpickle_version = cloudpickle.__version__
    if worker_cloudpickle_version != server_cloudpickle_version:
        raise FractalVersionMismatch(
            f"{server_cloudpickle_version=} but "
            f"{worker_cloudpickle_version=}"
        )

    server_fractal_server_version = server_versions["fractal_server"]
    worker_fractal_server_version = __VERSION__
    if worker_fractal_server_version != server_fractal_server_version:
        raise FractalVersionMismatch(
            f"{server_fractal_server_version=} but "
            f"{worker_fractal_server_version=}"
        )


def worker(
    *,
    in_fname: str,
    out_fname: str,
    extra_import_paths: Optional[str] = None,
) -> None:
    """
    Execute a job, possibly on a remote node.

    Arguments:
        in_fname: Absolute path to the input pickle file (must be readable).
        out_fname: Absolute path of the output pickle file (must be writeable).
        extra_import_paths: Additional import paths
    """

    # Create output folder, if missing
    out_dir = os.path.dirname(out_fname)
    if not os.path.exists(out_dir):
        logging.debug(f"_slurm.remote.worker: create {out_dir=}")
        os.mkdir(out_dir)

    if extra_import_paths:
        _extra_import_paths = extra_import_paths.split(":")
        sys.path[:0] = _extra_import_paths

    # Execute the job and catpure exceptions
    try:
        with open(in_fname, "rb") as f:
            indata = f.read()
        server_versions, fun, args, kwargs = cloudpickle.loads(indata)
        _check_versions_mismatch(server_versions)

        result = True, fun(*args, **kwargs)
        out = cloudpickle.dumps(result)
    except Exception as e:
        import traceback

        typ, value, tb = sys.exc_info()
        tb = tb.tb_next
        exc_proxy = ExceptionProxy(
            typ,
            "".join(traceback.format_exception(typ, value, tb)),
            *e.args,
            **e.__dict__,
        )

        result = False, exc_proxy
        out = cloudpickle.dumps(result)

    # Write the output pickle file
    tempfile = out_fname + ".tmp"
    with open(tempfile, "wb") as f:
        f.write(out)
    os.rename(tempfile, out_fname)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        type=str,
        help="Path of input pickle file",
        required=True,
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="Path of output pickle file",
        required=True,
    )
    parser.add_argument(
        "--extra-import-paths",
        type=str,
        help="Extra import paths",
        required=False,
    )
    parsed_args = parser.parse_args()
    logging.debug(f"{parsed_args=}")

    kwargs = dict(
        in_fname=parsed_args.input_file, out_fname=parsed_args.output_file
    )
    if parsed_args.extra_import_paths:
        kwargs["extra_import_paths"] = parsed_args.extra_import_paths
    worker(**kwargs)
