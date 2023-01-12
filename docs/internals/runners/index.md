# Runner Backends

Runner backends are responsible for scheduling and applying (running) tasks on
your data. Fractal currently supports two backends:

* [Process](process.md):
    This is the reference backend implementation. It is mostly useful for
    testing purposes, as it runs tasks locally on the same host where the
    server is installed..
* [SLURM](slurm.md):
    Run tasks by scheduling them on a SLURM cluster.
* PARSL:
    Run tasks through [Parsl](http://parsl-project.org) executors (**currently
    not supported**).

Both `Process` and `SLURM` backends leverage on Python's
[concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html)
interface. As such, writing a new backend based on concurrent executors should
require not much more effort than copying the reference `Process` interface
and swapping the `Executor` in the [public interface](#public-interface)
coroutine.

For this reason, both `Process` and `SLURM` backends largely build up on the
same set of common internal utilities and routines, c.f.,
[public](../../reference/fractal_server/app/runner/common/) and
[private](../../reference/fractal_server/app/runner/_common/) common backend
APIs.

## Public interface

The backends need to implement the following common public interface.

::: fractal_server.app.runner._process
    options:
        members:
            - process_workflow
