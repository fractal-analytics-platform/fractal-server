import pytest

from fractal_server.app.routes.admin.v2.task_group_recollect import (
    TaskGroupOverridesPyPI,
)
from fractal_server.app.schemas.v2.profile import (
    get_discriminator_value as get_2,
)
from fractal_server.app.schemas.v2.resource import ValidResourceBase
from fractal_server.app.schemas.v2.resource import (
    get_discriminator_value as get_1,
)


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


def test_recollection_schemas():
    obj = TaskGroupOverridesPyPI()
    assert obj.python_version is None
    assert obj.pip_extras is None
    assert obj.pinned_package_versions_pre == {}
    assert obj.pinned_package_versions_post == {}

    obj = TaskGroupOverridesPyPI(pinned_package_versions_pre="{}")
    assert obj.pinned_package_versions_pre == {}

    obj = TaskGroupOverridesPyPI(
        pinned_package_versions_pre='{"dask": "1.2.3"}',
        pinned_package_versions_post='{"zarr": "3.2.1"}',
    )
    assert obj.pinned_package_versions_pre == {"dask": "1.2.3"}
    assert obj.pinned_package_versions_post == {"zarr": "3.2.1"}

    with pytest.raises(ValueError, match="at least 1 character"):
        TaskGroupOverridesPyPI(pinned_package_versions_pre='{"dask": ""}')

    with pytest.raises(ValueError, match="should be a valid dictionary"):
        TaskGroupOverridesPyPI(pinned_package_versions_pre="[1, 2, 3]")
