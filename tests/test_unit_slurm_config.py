import json
from pathlib import Path

from devtools import debug

from fractal_server.app.runner._slurm import load_slurm_config


EXECUTOR = "cpu-low"
PARTITION = "main"
MEM = "7g"
NODES = "1"
NTASKS_PER_NODE = "1"
CPUS_PER_TASK = "1"


def test_load_slurm_config(tmp_path: Path):
    # Write the expected config to a JSON file
    expected_config = {
        EXECUTOR: {
            "partition": PARTITION,
            "mem": MEM,
            "nodes": NODES,
            "ntasks-per-node": NTASKS_PER_NODE,
            "cpus-per-task": CPUS_PER_TASK,
        }
    }
    expected_executor_config = expected_config[EXECUTOR]
    debug(expected_executor_config)
    config_path = tmp_path / "slurm_config.json"
    with config_path.open("w") as f:
        json.dump(expected_config, f)

    # Load the config through load_slurm_config
    config = load_slurm_config(config_path=config_path)
    executor_config = config[EXECUTOR].dict()
    debug(executor_config)

    # Assert that all expected attributes are present in the loaded config
    for key, value in expected_executor_config.items():
        key_slug = key.replace("-", "_")
        assert executor_config[key_slug] == value
