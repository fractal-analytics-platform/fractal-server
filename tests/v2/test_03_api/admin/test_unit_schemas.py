import pytest

from fractal_server.app.schemas.v2.profile import (
    get_discriminator_value as get_2,
)
from fractal_server.app.schemas.v2.resource import (
    get_discriminator_value as get_1,
)
from fractal_server.app.schemas.v2.resource import ValidResourceBase


def test_get_discriminator_value():
    # Dummy test, just for coverage
    class Mock(object):
        type: str = "type"
        resource_type: str = "resource_type"

    get_1(Mock())
    get_2(Mock())


def test_pixi_validator(slurm_ssh_resource_profile_fake_objects):
    res, prof = slurm_ssh_resource_profile_fake_objects
    res.tasks_pixi_config = dict(
        default_version="0.54.1",
        versions={"0.54.1": "/fake/0.54.1"},
    )
    with pytest.raises(ValueError, match="must include `SLURM_CONFIG`"):
        ValidResourceBase(**res.model_dump())
