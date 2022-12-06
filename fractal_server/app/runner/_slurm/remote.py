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
from typing import Optional

import cloudpickle


class ExceptionProxy:
    """
    Proxy class to serialise exceptions

    In general exceptions are not serialisable. This proxy class saves the
    serialisable content of an exception. On the receiving end, it can be used
    to reconstruct a TaskExecutionError.
    """

    def __init__(self, exc_type, tb, *args, **kwargs):
        self.exc_type_name = exc_type.__name__
        self.tb = tb
        self.args = args
        self.kwargs = kwargs


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
        import traceback

        typ, value, tb = sys.exc_info()
        tb = tb.tb_next
        exc_proxy = ExceptionProxy(
            typ,
            traceback.format_exception(typ, value, tb),
            e.args,
            **e.__dict__
        )

        result = False, exc_proxy
        out = cloudpickle.dumps(result)

    out_fname = in_fname.replace(".in.", ".out.")
    tempfile = out_fname + ".tmp"
    with open(tempfile, "wb") as f:
        f.write(out)
    os.rename(tempfile, out_fname)


if __name__ == "__main__":
    worker(*sys.argv[1:])
