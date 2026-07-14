"""
This module has the sole scope of exposing a mutable state for operations
related to `FRACTAL_LOG_CONFIG_FILE`.
"""

# Set to True when a FRACTAL_LOG_CONFIG_FILE file is loaded.
# When True, all functions that mutate logging state become no-ops so that
# the external dictConfig is the sole authority over the logging hierarchy.
_CONFIG_LOADED: bool = False
