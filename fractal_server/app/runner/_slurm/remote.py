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
import os
import sys
from typing import Dict
from typing import List
from typing import Optional
from typing import Type

import cloudpickle


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
        self, exc_type: Type[BaseException], tb: List[str], *args, **kwargs
    ):
        self.exc_type_name: str = exc_type.__name__
        self.tb: List[str] = tb
        self.args = args
        self.kwargs: Dict = kwargs


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
        fun, args, kwargs = cloudpickle.loads(indata)
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
    import logging

    logging.debug(f"{parsed_args=}")

    kwargs = dict(
        in_fname=parsed_args.input_file, out_fname=parsed_args.output_file
    )
    if parsed_args.extra_import_paths:
        kwargs["extra_import_paths"] = parsed_args.extra_import_paths
    worker(**kwargs)
