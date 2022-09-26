import json

import parsl
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
from sqlmodel import select

from fractal_server.app.models import Task
from fractal_server.app.models import TaskRead


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
    parsl.clear()
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


async def test_apply_workflow(
    db,
    client,
    collect_tasks,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    task_factory,
    tmp_path,
    patch_settings,
):
    """
    GIVEN
        * an input dataset and relative resource(s)
        * an output dataset and relative resource
        * a non-trivial workflow
    WHEN one applys the workflow to the input dataset
    THEN
        * the workflow is executed correctly
        * the output is correctly written in the output resource
    """

    from fractal_server.app.runner import submit_workflow

    # CREATE RESOURCES
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)
        ds = await dataset_factory(prj, type="image")
        out_ds = await dataset_factory(prj, type="image", name="out_ds")

        await resource_factory(ds)
        output_path = (tmp_path / "0.json").as_posix()
        await resource_factory(out_ds, path=output_path, glob_pattern=None)

    # CREATE NONTRIVIAL WORKFLOW
    wf = await task_factory(
        name="worfklow",
        module=None,
        resource_type="workflow",
        input_type="image",
    )

    stm = select(Task).where(Task.name == "dummy")
    res = await db.execute(stm)
    dummy_task = res.scalar()

    MESSAGE = "test apply workflow"
    await wf.add_subtask(db, subtask=dummy_task, args=dict(message=MESSAGE))
    debug(TaskRead.from_orm(wf))

    # DONE CREATING WORKFLOW

    await submit_workflow(
        db=db, input_dataset=ds, output_dataset=out_ds, workflow=wf
    )
    with open(output_path, "r") as f:
        data = json.load(f)
        debug(data)
    assert len(data) == 1
    assert data[0]["message"] == MESSAGE

    await db.refresh(out_ds)
    debug(out_ds)
    assert out_ds.meta
