# Runner Backends

Runner backends are responsible for actually scheduling and applying (running)
tasks on your data. Fractal currently supports two backends:

* [Process](process.md):
    This is the reference backend implementation. It is mostly useful for
    testing purposes, as it runs tasks locally on the same host as a server.
* [SLURM](slurm.md):
    Run tasks by scheduling them on a SLURM cluster.
* PARSL (deprecated):
    Run tasks through [Parsl](http://parsl-project.org/) executors.

## Public interface

The backends need to implement the following common public interface.

::: fractal_server.app.runner._process
    options:
        members:
            - process_workflow
