import parsl
from parsl.addresses import address_by_hostname
from parsl.app.python import PythonApp
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher
from parsl.channels.ssh.ssh import SSHChannel
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider

from fractal_server.app.runner.runner_utils import SrunLauncher


def hello():
    return 42


def test_parsl_local_config():
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
    parsl.load(Config(executors=[htex]))

    parsl_app = PythonApp(hello, executors=["local"])
    assert parsl_app().result() == 42


def test_parsl_slurm_config(ssh_params):
    provider_args = dict(
        partition="slurmpar",
        launcher=SrunLauncher(
            debug=False,
        ),
        channel=SSHChannel(**ssh_params, script_dir="/tmp/slurm_share/test0"),
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
