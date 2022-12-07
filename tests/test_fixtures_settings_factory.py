"""
These two tests simply check that the fixture `override_settings_factory`
restores the settings to the original values, so as to guarantee statelessness.
"""
import pytest
from devtools import debug

from fractal_server.config import get_settings
from fractal_server.syringe import Inject

new_runner_backend = "pippo"


@pytest.mark.skip("FIXME")
def test0(override_settings_factory):
    settings = Inject(get_settings)
    orig_runner_backend = settings.RUNNER_BACKEND
    debug(orig_runner_backend)

    override_settings_factory(RUNNER_BACKEND=new_runner_backend)
    settings = Inject(get_settings)
    assert settings.RUNNER_BACKEND == new_runner_backend
    debug("test0 done")


@pytest.mark.skip("FIXME")
def test1():
    settings = Inject(get_settings)
    orig_runner_backend = settings.RUNNER_BACKEND
    assert orig_runner_backend != new_runner_backend
