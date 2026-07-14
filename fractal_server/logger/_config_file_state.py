"""
This module has the sole scope of exposing a mutable state for operations
related to `LOG_CONFIG_FILE`.
"""

# Set to True when a LOG_CONFIG_FILE file is loaded.
# When True, all functions that mutate logging state become no-ops so that
# the external dictConfig is the sole authority over the logging hierarchy.
_CONFIG_LOADED: bool = False

# Set to the error message when LOG_CONFIG_FILE loading fails.
# When set, set_logger() will emit a warning on its first call so the
# failure is visible in the application logs.
_CONFIG_ERROR: str | None = None
