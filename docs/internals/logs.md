# Logs

Logging in `fractal-server` is based on the standard
[`logging`](https://docs.python.org/3/library/logging.html) library, and its
logging levels are defined
[here](https://docs.python.org/3/library/logging.html#logging-levels). For a
more detailed view on `fractal-server` logging, see the [logger module
documentation](../../reference/fractal_server/logger/).

The [logger module](../../reference/fractal_server/logger/) exposes the
functions to set/get/close a logger, and define where its records are sent to
(e.g. the `fractal-server` console or a specific file).  The logging levels of
a logger created with
[`set_logger`](../../reference/fractal_server/logger/#fractal_server.logger.set_logger)
are defined as follows:

* The minimal logging level for logs to appear in the console is set by
  [`FRACTAL_LOGGING_LEVEL`](../../configuration/#fractal_server.config.Settings.FRACTAL_LOGGING_LEVEL);
* The `FileHandler` logger handlers are alwasy set at the `DEBUG` level, that
  is, they write all log records.

This means that the `FRACTAL_LOGGING_LEVEL` offers a quick way to switch to
very verbose console logging (setting it e.g. to `10`, that is, `DEBUG` level)
and to switch back to less verbose logging (e.g. `FRACTAL_LOGGING_LEVEL=20` or
`30`), without ever modifying the on-file logs. Note that the typical reason
for having on-file logs in `fractal-server` is to log information about
background tasks, that are not executed as part of an API endpoint.


## Example use cases

1. Module-level logs that should only appear in the `fractal-server` console
```python
from fractal_server.logger import set_logger

module_logger = set_logger(__name__)

def my_function():
    module_logger.debug("This is an DEBUG log, from my_function")
    module_logger.info("This is an INFO log, from my_function")
    module_logger.warning("This is a WARNING log, from my_function")
```
Note that only logs with level equal or higher to `FRACTAL_LOGGING_LEVEL` will be shown.

2. Function-level logs that should only appear in the `fractal-server` console
```python
from fractal_server.logger import set_logger

def my_function():
    function_logger = set_logger("my_function")
    function_logger.debug("This is an DEBUG log, from my_function")
    function_logger.info("This is an INFO log, from my_function")
    function_logger.warning("This is a WARNING log, from my_function")
```
Note that only logs with level equal or higher to `FRACTAL_LOGGING_LEVEL` will be shown.

3. Custom logs that should appear both in the fractal-server console and in a
   log file
```python
from fractal_server.logger import set_logger
from fractal_server.logger import close_logger

def my_function():
    this_logger = set_logger("this_logger", log_file_path="/tmp/this.log")
    this_logger.debug("This is an DEBUG log, from my_function")
    this_logger.info("This is an INFO log, from my_function")
    this_logger.warning("This is a WARNING log, from my_function")
    close_logger(this_logger)
```
Note that only logs with level equal or higher to `FRACTAL_LOGGING_LEVEL` will
be shown in the console, but *all* logs will be written to `"/tmp/this.log"`.

## Future plans

The current page concerns the logs that are emitted from `fractal-sever`, but
not the ones coming from other sources (e.g. `fastapi` or `uvicorn/gunicorn`).
In a [future
refactor](https://github.com/fractal-analytics-platform/fractal-server/issues/620)
we may address this point, with the twofold goal of

1. Integrating different log sources, so that they can be shown in a
   homogeneous way (e.g. all with same format);
2. Redirecting all console logs (from different sources) to a rotating file
   (e.g. via a
[RotatingFileHandler](https://docs.python.org/3/library/logging.handlers.html#rotatingfilehandler)).
