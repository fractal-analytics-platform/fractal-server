import parsl
from parsl.addresses import address_by_hostname
from parsl.app.python import PythonApp
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SingleNodeLauncher
from parsl.providers import LocalProvider
from parsl.providers import SlurmProvider

from fractal_server.app.runner.runner_utils import UserSrunLauncher


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


def test_parsl_slurm_config():
    provider_args = dict(
        partition="main",
        launcher=UserSrunLauncher(debug=False),
        channel=LocalChannel(),
        nodes_per_block=1,
        init_blocks=1,
        min_blocks=0,
        max_blocks=4,
        walltime="10:00:00",
    )
    prov_slurm_cpu = SlurmProvider(**provider_args)

    htex = HighThroughputExecutor(
        label="executor",
        provider=prov_slurm_cpu,
        address="0.0.0.0:6817",  # address_by_hostname(),
    )

    parsl.load(Config(executors=[htex]))
    # dfk = DataFlowKernelLoader.dfk()
    # dfk.add_executors([htex])

    parsl_app = PythonApp(hello, executors=["executor"])
    print(parsl_app().result())

    assert False
