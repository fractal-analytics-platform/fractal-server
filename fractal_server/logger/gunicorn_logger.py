"""
This module (which is only executed if `gunicorn` can be imported) subclasses
the gunicorn `Logger` class in order to slightly change its log formats.

This class can be used by including this `gunicorn` command-line option:
```
--logger-class fractal_server.logger.gunicorn_logger.FractalGunicornLogger
```
"""

try:
    from gunicorn.glogging import Logger as GunicornLogger

    class FractalGunicornLogger(GunicornLogger):
        error_fmt = r"%(asctime)s   - gunicorn.error - %(levelname)s - [pid %(process)d] - %(message)s"  # noqa: E501
        datefmt = r"%Y-%m-%d %H:%M:%S,%u"

except (ModuleNotFoundError, ImportError):
    pass
