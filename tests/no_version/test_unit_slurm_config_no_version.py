import pytest
from devtools import debug

from fractal_server.app.runner.executors.slurm._slurm_config import (
    _parse_mem_value,
)
from fractal_server.app.runner.executors.slurm._slurm_config import (
    get_default_slurm_config,
)
from fractal_server.app.runner.executors.slurm._slurm_config import (
    SlurmConfigError,
)


def test_to_sbatch_preamble():
    """
    Given a SlurmConfig object, test its to_sbatch_preamble method
    """

    slurm_config = get_default_slurm_config()
    debug(slurm_config)

    GRES = "some-gres"
    EXTRA_LINES = [
        "export VAR2=2",
        "#SBATCH --optionA=valueA",
        "export VAR1=1",
        "#SBATCH --optionB=valueB",
    ]
    MEM_SINGLE_TASK_MB = 100

    slurm_config.mem_per_task_MB = MEM_SINGLE_TASK_MB
    slurm_config.parallel_tasks_per_job = 3
    slurm_config.tasks_per_job = 5
    slurm_config.cpus_per_task = 2
    slurm_config.gres = GRES
    slurm_config.extra_lines = EXTRA_LINES
    preamble = slurm_config.to_sbatch_preamble()
    debug(preamble)

    assert preamble[0] == "#!/bin/sh"
    assert f"#SBATCH --gres={GRES}" in preamble
    for line in EXTRA_LINES:
        assert line in preamble
    MEM = MEM_SINGLE_TASK_MB * slurm_config.parallel_tasks_per_job
    assert f"#SBATCH --mem={MEM}M" in preamble


def test_parse_mem_value():

    # Successful calls
    assert _parse_mem_value(99) == 99
    assert _parse_mem_value("99") == 99
    assert _parse_mem_value("99M") == 99
    assert _parse_mem_value("2G") == 2000
    assert _parse_mem_value("3T") == 3000000

    # Failures
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("100 M")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("100K")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("M10M")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("10-M")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("2.5G")


def test_to_sbatch_preamble_with_user_local_exports():
    slurm_config = get_default_slurm_config()
    slurm_config.parallel_tasks_per_job = 1
    slurm_config.tasks_per_job = 1
    slurm_config.user_local_exports = dict(
        CELLPOSE_LOCAL_MODELS_PATH="CELLPOSE_LOCAL_MODELS_PATH",
        NAPARI_CONFIG="napari_config.json",
    )
    debug(slurm_config)

    expected_line_1 = (
        "export CELLPOSE_LOCAL_MODELS_PATH="
        "/some/path/CELLPOSE_LOCAL_MODELS_PATH"
    )
    expected_line_2 = "export NAPARI_CONFIG=/some/path/napari_config.json"

    # Test that user_cache_dir is required
    with pytest.raises(ValueError):
        preamble = slurm_config.to_sbatch_preamble()

    # Test preamble (without trailing slash in user_cache_dir)
    CACHE = "/some/path"
    preamble = slurm_config.to_sbatch_preamble(remote_export_dir=CACHE)
    debug(preamble)
    assert expected_line_1 in preamble
    assert expected_line_2 in preamble

    # Test preamble (without trailing slash in user_cache_dir)
    CACHE = "/some/path/"
    preamble = slurm_config.to_sbatch_preamble(remote_export_dir=CACHE)
    debug(preamble)
    assert expected_line_1 in preamble
    assert expected_line_2 in preamble
