# Runner Backends

Runner backends are responsible for scheduling and applying (running) tasks on your data. Fractal currently supports the integration with a SLURM cluster, with user impersonation handled either via `sudo -u` or by `ssh`. More details [here](slurm.md).

Moreover, a [`local` backend](local.md) exists for development and testing. This backend runs tasks locally, on the same host where `fractal-server` is running, and through a [concurrent.futures](https://docs.python.org/3/library/concurrent.futures.html) `ThreadPoolExecutor`.
