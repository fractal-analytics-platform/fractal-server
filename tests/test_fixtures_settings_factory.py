"""
These two tests simply check that the fixture `override_settings_factory`
restores the settings to the original values, so as to guarantee statelessness.
"""
from devtools import debug

from fractal_server.config import get_settings

new_runner_backend = "pippo"


def test0(override_settings_factory):
    settings = get_settings()
    orig_runner_backend = settings.FRACTAL_RUNNER_BACKEND
    debug(orig_runner_backend)

    override_settings_factory(FRACTAL_RUNNER_BACKEND=new_runner_backend)
    settings = get_settings()
    assert settings.FRACTAL_RUNNER_BACKEND == new_runner_backend
    debug("test0 done")


def test1():
    settings = get_settings()
    orig_runner_backend = settings.FRACTAL_RUNNER_BACKEND
    assert orig_runner_backend != new_runner_backend
