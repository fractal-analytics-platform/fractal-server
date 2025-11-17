"""
These two tests simply check that the fixture `override_settings_factory`
restores the settings to the original values, so as to guarantee statelessness.
"""

import pytest

from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

NEW_RUNNER_BACKEND = ResourceType.SLURM_SUDO
NEW_JWT_EXPIRE_SECONDS = 1234


def test_override_settings_factory_0(override_settings_factory):
    # First override / success
    override_settings_factory(FRACTAL_RUNNER_BACKEND=NEW_RUNNER_BACKEND)
    settings = Inject(get_settings)
    assert settings.FRACTAL_RUNNER_BACKEND == NEW_RUNNER_BACKEND

    # Second override / fail due to invalid data
    with pytest.raises(ValueError):
        override_settings_factory(FRACTAL_RUNNER_BACKEND="invalid-value")

    # Third override / success
    override_settings_factory(JWT_EXPIRE_SECONDS=NEW_JWT_EXPIRE_SECONDS)
    settings = Inject(get_settings)
    assert settings.JWT_EXPIRE_SECONDS == NEW_JWT_EXPIRE_SECONDS
    assert settings.FRACTAL_RUNNER_BACKEND == NEW_RUNNER_BACKEND


def test_override_settings_factory_1():
    """
    Run after test 0 above, to verify that it is stateless.
    """
    settings = Inject(get_settings)
    assert settings.FRACTAL_RUNNER_BACKEND != NEW_RUNNER_BACKEND
    assert settings.JWT_EXPIRE_SECONDS != NEW_JWT_EXPIRE_SECONDS
