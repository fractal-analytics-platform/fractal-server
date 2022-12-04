# This adapts clusterfutures <https://github.com/sampsyo/clusterfutures>
# Original Copyright
# Copyright 2021 Adrian Sampson <asampson@cs.washington.edu>
# License: MIT
#
# Modified by:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
#
# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
"""
This module provides a simple self-standing script that executes arbitrary
python code received via pickled files on a cluster node.
"""
import os
import sys
from pickle import PicklingError
from typing import Optional

import cloudpickle


def worker(in_fname: str, extra_import_paths: Optional[str] = None):
    """Called to execute a job on a remote host."""
    if extra_import_paths:
        _extra_import_paths = extra_import_paths.split(":")
        sys.path[:0] = _extra_import_paths

    try:
        with open(in_fname, "rb") as f:
            indata = f.read()
        fun, args, kwargs = cloudpickle.loads(indata)
        result = True, fun(*args, **kwargs)
        out = cloudpickle.dumps(result)
    except Exception as e:
        try:
            result = False, e
            out = cloudpickle.dumps(result)
        except PicklingError:
            result = False, e.__traceback__

    out_fname = in_fname.replace(".in.", ".out.")
    tempfile = out_fname + ".tmp"
    with open(tempfile, "wb") as f:
        f.write(out)
    os.rename(tempfile, out_fname)


if __name__ == "__main__":
    worker(*sys.argv[1:])
