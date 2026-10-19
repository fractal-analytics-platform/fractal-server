"""
Backend (`process_workflow` function and runner class) of a resource.
"""

from collections.abc import Callable
from dataclasses import dataclass

from fractal_server.app.models.v2 import Resource
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.runner.executors.base_runner import BaseRunner
from fractal_server.runner.executors.local.runner import LocalRunner
from fractal_server.runner.executors.slurm_rest.runner import SlurmRestRunner
from fractal_server.runner.executors.slurm_rest.settings import (
    is_slurm_rest_enabled,
)
from fractal_server.runner.executors.slurm_ssh.runner import SlurmSSHRunner
from fractal_server.runner.executors.slurm_sudo.runner import SlurmSudoRunner

from ._local import process_workflow as local_process_workflow
from ._slurm_rest import process_workflow as slurm_rest_process_workflow
from ._slurm_ssh import process_workflow as slurm_ssh_process_workflow
from ._slurm_sudo import process_workflow as slurm_sudo_process_workflow


@dataclass(frozen=True)
class Backend:
    process_workflow: Callable[..., None]
    runner_class: type[BaseRunner]


def get_backend(resource: Resource) -> Backend:
    match resource.type:
        case ResourceType.LOCAL:
            return Backend(local_process_workflow, LocalRunner)
        case ResourceType.SLURM_SUDO:
            return Backend(slurm_sudo_process_workflow, SlurmSudoRunner)
        case ResourceType.SLURM_SSH:
            if is_slurm_rest_enabled():
                return Backend(slurm_rest_process_workflow, SlurmRestRunner)
            return Backend(slurm_ssh_process_workflow, SlurmSSHRunner)
        case _:
            raise ValueError(f"Unknown resource type '{resource.type}'.")
