import pytest

from fractal_server.runner.config.slurm_mem_to_MB import slurm_mem_to_MB
from fractal_server.runner.config.slurm_mem_to_MB import SlurmConfigError


def test_slurm_mem_to_MB():
    assert slurm_mem_to_MB(1) == 1
    assert slurm_mem_to_MB("1") == 1
    assert slurm_mem_to_MB("1M") == 1
    assert slurm_mem_to_MB("1G") == 10**3
    assert slurm_mem_to_MB("1T") == 10**6

    with pytest.raises(SlurmConfigError):
        slurm_mem_to_MB("ABC")
    with pytest.raises(SlurmConfigError):
        slurm_mem_to_MB("AM")
    with pytest.raises(SlurmConfigError):
        slurm_mem_to_MB("AG")
    with pytest.raises(SlurmConfigError):
        slurm_mem_to_MB("AT")
    with pytest.raises(SlurmConfigError):
        slurm_mem_to_MB("1A")
