# Logging

Logging in `fractal-server` is based on the standard
[`logging`](https://docs.python.org/3/library/logging.html) library, and its
logging levels are defined
[here](https://docs.python.org/3/library/logging.html#logging-levels).

Two different approaches to logging configuration are described below. For a more detailed view on `fractal-server` logging, see the [logger module
documentation](../code_reference/logger/).

## External config file

Set the `FRACTAL_LOG_CONFIG_FILE` environment variable to the path of a YAML
file containing a standard Python
[`logging.config.dictConfig`](https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig)
configuration. When this variable is set, it is the only one determining
application-level logging: the `FRACTAL_LOGGING_LEVEL` setting and
programmatic stream-handler setup (`config_uvicorn_loggers()`, and
`set_logger()` calls without a `log_file_path`) become no-ops, and the YAML
file is the sole authority over the logging hierarchy.

**Exception - job log files:** calls to `set_logger()` that include a
`log_file_path` argument always create the corresponding `FileHandler`, even
when an external config is loaded. This is because certain log files
(e.g. `workflow.log` and task-collection logs) are functional artifacts that
are read back into the database and must always be written, independently of
how application logging is configured. Consequently, `close_logger()` and
`reset_logger_handlers()` also always clean up any `FileHandler`s, even in
external-config mode.

This mode enables fine-grained control, including multiple rotating log files
split by severity level (debug / info / warning / error) and separate access
logs for Uvicorn.

## Built-in logger (default)

The [logger module](../code_reference/logger/) exposes the
functions to set/get/close a logger, and it defines where the records are sent to
(e.g. the `fractal-server` console or a specific file). The logging levels of
a logger created with
[`set_logger`](../code_reference/logger#fractal_server.logger.set_logger)
are defined as follows:

* The minimum logging level for logs to appear in the console is set by
  [`FRACTAL_LOGGING_LEVEL`](../configuration.md#fractal_server.config._main.Settings);
* The `FileHandler` logger handlers are always set at the `DEBUG` level, that
  is, they write all log records.

This means that the `FRACTAL_LOGGING_LEVEL` offers a quick way to switch to
very verbose console logging (setting it e.g. to `10`, that is, `DEBUG` level)
and to switch back to less verbose logging (e.g. `FRACTAL_LOGGING_LEVEL=20` or
`30`), without ever modifying the on-file logs. Note that the typical reason
for having on-file logs in `fractal-server` is to log information about
background tasks, that are not executed as part of an API endpoint.


### Example use cases

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
