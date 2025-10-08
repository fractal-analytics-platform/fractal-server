"""
These two tests simply check that the fixture `override_settings_factory`
restores the settings to the original values, so as to guarantee statelessness.
"""
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

NEW_RUNNER_BACKEND = "slurm_sudo"


def test_override_settings_factory_0(override_settings_factory):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=NEW_RUNNER_BACKEND)
    settings = Inject(get_settings)
    assert settings.FRACTAL_RUNNER_BACKEND == NEW_RUNNER_BACKEND


def test_override_settings_factory_1():
    """
    Run after test 0 above, to verify that it is stateless.
    """
    settings = Inject(get_settings)
    orig_runner_backend = settings.FRACTAL_RUNNER_BACKEND
    assert orig_runner_backend != NEW_RUNNER_BACKEND
