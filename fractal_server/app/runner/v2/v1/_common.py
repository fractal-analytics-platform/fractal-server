"""
Common utilities and routines for runner backends (private API)

This module includes utilities and routines that are of use to implement
runner backends and that should not be exposed outside of the runner
subsystem.
"""
from pathlib import Path

from ....models import WorkflowTask


def no_op_submit_setup_call(
    *,
    wftask: WorkflowTask,
    workflow_dir: Path,
    workflow_dir_user: Path,
) -> dict:
    """
    Default (no-operation) interface of submit_setup_call.
    """
    return {}
