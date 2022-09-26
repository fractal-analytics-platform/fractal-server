"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original authors:
Marco Franzon <marco.franzon@exact-lab.it>
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>


This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import asyncio
from functools import partial
from functools import wraps
from logging import FileHandler
from logging import Formatter
from logging import getLogger
from typing import Callable

import parsl
from parsl import channels as parsl_channels
from parsl import launchers as parsl_launchers
from parsl import providers as parsl_providers
from parsl.addresses import address_by_hostname
from parsl.config import Config
from parsl.dataflow.dflow import DataFlowKernelLoader
from parsl.executors import HighThroughputExecutor
from parsl.monitoring.monitoring import MonitoringHub

from ...config import settings


formatter = Formatter("%(asctime)s; %(levelname)s; %(message)s")

logger = getLogger(__name__)
handler = FileHandler("parsl_executors.log", mode="a")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel("INFO")


def add_prefix(*, workflow_id: int, executor_label: str):
    return f"{workflow_id}___{executor_label}"


def async_wrap(func: Callable) -> Callable:
    """
    See issue #140 and https://stackoverflow.com/q/43241221/19085332

    By replacing
        .. = final_metadata.result()
    with
        .. = await async_wrap(get_app_future_result)(app_future=final_metadata)
    we avoid a (long) blocking statement.
    """

    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


class ParslConfiguration:
    def __init__(self):
        self._launcher_dict = {}
        self._channel_dict = {}
        self._provider_dict = {}
        self._executor_dict = {}

    def add_launcher(self, name: str, type: str, **kwargs):
        self._launcher_dict[name] = getattr(parsl_launchers, type)(**kwargs)

    def add_provider(
        self,
        *,
        name: str,
        launcher_name: str,
        channel_name: str,
        type: str,
        **kwargs,
    ):
        self._provider_dict[name] = getattr(parsl_providers, type)(
            launcher=self._launcher_dict[launcher_name],
            channel=self._channel_dict[channel_name],
            **kwargs,
        )

    def add_channel(self, name: str, type: str, **kwargs):
        self._channel_dict[name] = getattr(parsl_channels, type)(**kwargs)

    def add_executor(
        self,
        *,
        name: str,
        provider_name: str,
        type: str,
        workflow_id: str,
        **kwargs,
    ):
        if type == "HighThroughputExecutor":
            self._executor_dict[name] = HighThroughputExecutor(
                provider=self._provider_dict[provider_name],
                label=add_prefix(workflow_id=workflow_id, executor_label=name),
                **kwargs,
            )
        else:
            raise NotImplementedError

    @property
    def executors(self):
        return list(self._executor_dict.values())

    @property
    def executor_labels(self):
        return list(self._executor_dict.keys())


def generate_parsl_config(
    *,
    workflow_id: int,
) -> ParslConfiguration:
    config = settings.PARSL_CONFIG
    logger.info(f"settings.PARSL_CONFIG: {config}")

    allowed_configs = ["local", "pelkmanslab", "custom"]
    if config not in allowed_configs:
        raise ValueError(f"{config=} not in {allowed_configs=}")

    parsl_config = ParslConfiguration()

    if config == "local":
        parsl_config.add_launcher(
            name="default", type="SingleNodeLauncher", debug=False
        )
        parsl_config.add_channel(name="default", type="LocalChannel")
        parsl_config.add_provider(
            name="default",
            type="LocalProvider",
            launcher_name="default",
            channel_name="default",
            init_blocks=1,
            min_blocks=0,
            max_blocks=4,
        )
        for name in ["cpu-low", "cpu-mid", "cpu-high", "gpu"]:
            parsl_config.add_executor(
                name=name,
                type="HighThroughputExecutor",
                workflow_id=workflow_id,
                provider_name="default",
                address=address_by_hostname(),
            )

    elif config == "pelkmanslab":
        parsl_config.add_launcher(
            name="srun_launcher",
            type="SrunLauncher",
            debug=False,
        )
        parsl_config.add_channel(
            name="default",
            type="LocalChannel",
        )

        common_provider_args = dict(
            partition="main",
            nodes_per_block=1,
            init_blocks=1,
            min_blocks=0,
            max_blocks=100,
            parallelism=1,
            exclusive=False,
            walltime="20:00:00",
        )

        parsl_config.add_provider(
            name="prov_cpu_low",
            type="SlurmProvider",
            launcher_name="srun_launcher",
            channel_name="default",
            cores_per_node=1,
            mem_per_node=7,
            **common_provider_args,
        )
        parsl_config.add_provider(
            name="prov_cpu_mid",
            type="SlurmProvider",
            launcher_name="srun_launcher",
            channel_name="default",
            cores_per_node=4,
            mem_per_node=15,
            **common_provider_args,
        )
        parsl_config.add_provider(
            name="prov_cpu_high",
            type="SlurmProvider",
            launcher_name="srun_launcher",
            channel_name="default",
            cores_per_node=16,
            mem_per_node=61,
            **common_provider_args,
        )
        parsl_config.add_provider(
            name="prov_gpu",
            type="SlurmProvider",
            launcher_name="srun_launcher",
            channel_name="default",
            partition="gpu",
            nodes_per_block=1,
            init_blocks=1,
            min_blocks=0,
            max_blocks=2,
            mem_per_node=61,
            walltime="10:00:00",
        )

        # Define executors
        parsl_config.add_executor(
            name="cpu-low",
            type="HighThroughputExecutor",
            workflow_id=workflow_id,
            provider_name="prov_cpu_low",
            address=address_by_hostname(),
            cpu_affinity="block",
            max_workers=100,
            mem_per_worker=7,
        )
        parsl_config.add_executor(
            name="cpu-mid",
            type="HighThroughputExecutor",
            workflow_id=workflow_id,
            provider_name="prov_cpu_mid",
            address=address_by_hostname(),
            cpu_affinity="block",
            max_workers=100,
            mem_per_worker=15,
        )
        parsl_config.add_executor(
            name="cpu-high",
            type="HighThroughputExecutor",
            workflow_id=workflow_id,
            provider_name="prov_cpu_high",
            address=address_by_hostname(),
            cpu_affinity="block",
            max_workers=100,
            mem_per_worker=61,
        )
        parsl_config.add_executor(
            name="gpu",
            type="HighThroughputExecutor",
            workflow_id=workflow_id,
            provider_name="prov_gpu",
            address=address_by_hostname(),
            cpu_affinity="block",
            max_workers=100,
            mem_per_worker=61,
        )
    return parsl_config


def load_parsl_config(
    *,
    parsl_config: ParslConfiguration,
    enable_monitoring: bool = True,
) -> None:

    # Define monitoring hub and finalize configuration
    if enable_monitoring:
        monitoring = MonitoringHub(
            hub_address=address_by_hostname(),
            workflow_name="fractal",
        )
    else:
        monitoring = None

    try:
        dfk = DataFlowKernelLoader.dfk()
        old_executor_labels = [
            executor_label for executor_label in dfk.executors.keys()
        ]
        logger.info(
            f"DFK {dfk} exists, with {len(dfk.executors)} executors: "
            f"{old_executor_labels}"
        )
        logger.info(
            f"Adding {len(parsl_config.executors)} new executors: "
            f"{parsl_config.executor_labels}"
        )

        # FIXME: what if an executor was already there?
        # (re-submitting same workflow?)

        dfk.add_executors(parsl_config.executors)

    # FIXME: better exception handling
    except RuntimeError:
        config = Config(
            executors=parsl_config.executors,
            monitoring=monitoring,
            max_idletime=20.0,
        )
        logger.info(
            "DFK probably missing, "
            "proceed with parsl.clear and parsl.config.Config"
        )
        parsl.clear()
        parsl.load(config)
    dfk = DataFlowKernelLoader.dfk()
    executor_labels = [
        executor_label for executor_label in dfk.executors.keys()
    ]
    logger.info(
        f"DFK {dfk} now has {len(parsl_config.executor_labels)} executors: "
        f"{executor_labels}"
    )


def shutdown_executors(*, workflow_id: str):
    # Remove executors from parsl DFK
    # FIXME decorate with monitoring logs, as in:
    # https://github.com/Parsl/parsl/blob/master/parsl/dataflow/dflow.py#L1106
    dfk = DataFlowKernelLoader.dfk()
    for label, executor in dfk.executors.items():
        if label.startswith(f"{workflow_id}___"):
            executor.shutdown()
            logger.info(f"SHUTTING DOWN {label}")
    executor_labels = [
        executor_label for executor_label in dfk.executors.keys()
    ]
    logger.info(
        f"DFK {dfk} now has {len(executor_labels)} executors: "
        f"{executor_labels}"
    )


def get_unique_executor(*, workflow_id: int, task_executor: str = None):

    # Handle missing value
    if task_executor is None:
        task_executor = settings.PARSL_DEFAULT_EXECUTOR

    # Redefine task_executor, by prepending workflow_id
    new_task_executor = add_prefix(
        workflow_id=workflow_id, executor_label=task_executor
    )

    # Verify match between new_task_executor and available executors
    valid_executor_labels = DataFlowKernelLoader.dfk().executors.keys()
    if new_task_executor not in valid_executor_labels:
        raise ValueError(
            f"Executor label {new_task_executor} is not in "
            f"{valid_executor_labels=}"
        )

    return new_task_executor
