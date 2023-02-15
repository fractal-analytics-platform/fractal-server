import sys
import traceback

import pytest
from devtools import debug

from fractal_server.app.runner._slurm.remote import ExceptionProxy
from fractal_server.app.runner.common import TaskExecutionError

# Docs for traceback.format_exception:
# Format a stack trace and the exception information. [..] The return value is
# a list of strings, each ending in a newline and some containing internal
# newlines.
# (https://docs.python.org/3/library/traceback.html#traceback.format_exception)


@pytest.mark.skip()
def test_ExceptionProxy():

    # When serializing an exception into an ExceptionProxy object, make sure
    # that the __repr__ or __str__ or show method returns a single string FIXME

    # Function that yields a non-trivial error traceback
    def call_single_task():
        def _call_command_wrapper():
            stderr = "This is a stderr message, single line"
            raise TaskExecutionError(
                stderr,
                workflow_task_id=1,
                task_name="My Task",
                workflow_task_order=0,
            )

        _call_command_wrapper()

    # WORKER SIDE

    # Catch and handle the exception
    try:
        call_single_task()
    except Exception as e:
        typ, value, tb = sys.exc_info()
        tb = tb.tb_next
        formatted_tb = traceback.format_exception(typ, value, tb)
        formatted_tb = "".join(formatted_tb)
        print(formatted_tb)
        proxy = ExceptionProxy(
            typ,
            formatted_tb,
            *e.args,
            **e.__dict__,
        )

    # EXECUTOR SIDE
    exc = TaskExecutionError(
        proxy.tb,
        *proxy.args,
        *proxy.kwargs,
    )
    try:
        raise exc
    except TaskExecutionError as e:
        # print('-' * 80)
        # debug(e.__repr__())
        # print(e.__repr__())
        # print()
        print("-" * 80)
        debug(e.__str__())
        print(e.__str__())
        print()
        print("-" * 80)
        debug(e.__str__().replace("\\n", "\n"))
        print(e.__str__().replace("\\n", "\n"))
        print()
