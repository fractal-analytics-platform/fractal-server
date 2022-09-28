import json
from pathlib import Path

import parsl
import pytest
from devtools import debug
from parsl.addresses import address_by_hostname
from parsl.app.python import PythonApp
from parsl.channels import LocalChannel
from parsl.channels.ssh.ssh import SSHChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher
from parsl.launchers import SrunLauncher
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider

from .fixtures_workflow import LEN_NONTRIVIAL_WORKFLOW
from fractal_server.app.runner.parsl_runner import _process_workflow
from fractal_server.app.runner.runner_utils import ParslConfiguration


# Check that test environment is available at localhost:10022
def check_docker_slurm():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    check = s.connect_ex(("127.0.0.1", 10022))
    return check == 0  # port is open


HAS_DOCKER_SLURM = check_docker_slurm()


def hello():
    return 42


def test_parsl_local_config():
    """
    GIVEN a function
    WHEN Parsl is configured to run locally
    THEN the function can be executed
    """
    prov = LocalProvider(
        launcher=SingleNodeLauncher(debug=False),
        channel=LocalChannel(),
        init_blocks=1,
        min_blocks=0,
        max_blocks=4,
    )

    # Define two identical (apart from the label) executors
    htex = HighThroughputExecutor(
        label="local",
        provider=prov,
        address=address_by_hostname(),
    )
    parsl.clear()
    parsl.load(Config(executors=[htex]))

    parsl_app = PythonApp(hello, executors=["local"])
    assert parsl_app().result() == 42


@pytest.mark.skipif(not HAS_DOCKER_SLURM, reason="no dockerised slurm cluster")
def test_parsl_slurm_config(ssh_params):
    """
    GIVEN
        * a dockerised slurm cluster (cf. `docker-slurm.yaml`)
        * on which there exists a user `test0` (cf. `ssh_params` fixture)
        * the master node is reachable via SSH
        * a shared volume mounted on /tmp/slurm_share
    WHEN Parsl is configured to run on Slurm via SSH
    THEN a function can be executed
    """
    provider_args = dict(
        partition="slurmpar",
        launcher=SrunLauncher(
            debug=False,
        ),
        channel=SSHChannel(**ssh_params, script_dir="/tmp/slurm_share/test0/"),
        nodes_per_block=1,
        init_blocks=1,
        min_blocks=0,
        max_blocks=1,
        walltime="10:00:00",
    )
    prov_slurm_cpu = SlurmProvider(**provider_args)

    htex = HighThroughputExecutor(
        label="parsl_executor",
        provider=prov_slurm_cpu,
        # this address is necessary so that the slurm node within the container
        # be able to contact the interchange on the host machine.
        # In general this is the address of the machine on which the fractal
        # server runs
        address="host.docker.internal",
        working_dir="/tmp/slurm_share/test0/work",
        worker_logdir_root="/tmp/slurm_share/test0",
    )

    parsl.clear()
    parsl.load(Config(executors=[htex]))

    # NOTE:
    # Using cos instead of hello because otherwise we need to install
    # fractal-server cluster-wide. This might still be desirable at the end.
    from math import cos

    parsl_app = PythonApp(cos, executors=["parsl_executor"])

    result = parsl_app(0).result()
    assert result == 1.0


async def test_apply_workflow_slurm(
    nontrivial_workflow, patch_settings, ssh_params
):
    """
    GIVEN a nontrivial workflow
    WHEN the workflow is processed
    THEN
        * a single PARSL python_app which will execute the workflow is produced
        * it is executable
        * the output is the one expected from the workflow
    """

    slurm_shared_path = Path("/tmp/slurm_share/test0/")

    parsl_config = ParslConfiguration()
    parsl_config.add_launcher(name="srun", type="SrunLauncher")
    parsl_config.add_channel(
        name="ssh_channel",
        type="SSHChannel",
        script_dir="/tmp/slurm_share/test0",
        **ssh_params
    )
    parsl_config.add_provider(
        name="slurm_provider",
        type="SlurmProvider",
        partition="slurmpar",
        launcher_name="srun",
        channel_name="ssh_channel",
    )
    nontrivial_workflow.id = 123

    parsl_config.add_executor(
        # Using `cpu-low` as it is currently the default executor for dummy
        name="cpu-low",
        type="HighThroughputExecutor",
        provider_name="slurm_provider",
        address="host.docker.internal",
        working_dir="/tmp/slurm_share/test0/work/",
        worker_logdir_root="/tmp/slurm_share/test0",
        workflow_id=nontrivial_workflow.id,
    )

    app = _process_workflow(
        task=nontrivial_workflow,
        input_paths=[slurm_shared_path / "0.json"],
        output_path=slurm_shared_path / "0.json",
        metadata={},
        parsl_config=parsl_config,
    )
    debug(app)
    app.result()

    print(list(slurm_shared_path.glob("*.json")))
    for f in slurm_shared_path.glob("*.json"):
        with open(f, "r") as output_file:
            data = json.load(output_file)
            debug(data)
    assert len(data) == LEN_NONTRIVIAL_WORKFLOW
    assert data[0]["message"] == "dummy0"
    assert data[1]["message"] == "dummy1"
    assert data[2]["message"] == "dummy2"
