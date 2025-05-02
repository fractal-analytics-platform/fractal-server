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
from typing import Union

import cloudpickle

from fractal_server import __VERSION__


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

    server_python_version = list(server_versions["python"])
    worker_python_version = list(sys.version_info[:3])
    if worker_python_version != server_python_version:
        if worker_python_version[:2] != server_python_version[:2]:
            # FIXME: Turn this into an error, in some version post 2.14.
            logging.error(
                f"{server_python_version=} but {worker_python_version=}. "
                "This configuration will be deprecated in a future version, "
                "please contact the admin of this Fractal instance."
            )
        else:
            # Major.minor versions match, patch versions differ
            logging.warning(
                f"{server_python_version=} but {worker_python_version=}."
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

    # Execute the job and capture exceptions
    try:
        with open(in_fname, "rb") as f:
            indata = f.read()
        server_versions, fun, args, kwargs = cloudpickle.loads(indata)
        _check_versions_mismatch(server_versions)

        result = (True, fun(*args, **kwargs))
        out = cloudpickle.dumps(result)
    except Exception as e:
        # Exception objects are not serialisable. Here we save the relevant
        # exception contents in a serializable dictionary. Note that whenever
        # the task failed "properly", the exception is a `TaskExecutionError`
        # and it has additional attributes.

        import traceback

        exc_type, exc_value, traceback_obj = sys.exc_info()
        traceback_obj = traceback_obj.tb_next
        traceback_list = traceback.format_exception(
            exc_type,
            exc_value,
            traceback_obj,
        )
        traceback_string = "".join(traceback_list)
        exc_proxy = dict(
            exc_type_name=exc_type.__name__,
            traceback_string=traceback_string,
            workflow_task_order=getattr(e, "workflow_task_order", None),
            workflow_task_id=getattr(e, "workflow_task_id", None),
            task_name=getattr(e, "task_name", None),
        )
        result = (False, exc_proxy)
        out = cloudpickle.dumps(result)

    # Write the output pickle file
    with open(out_fname, "wb") as f:
        f.write(out)


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
