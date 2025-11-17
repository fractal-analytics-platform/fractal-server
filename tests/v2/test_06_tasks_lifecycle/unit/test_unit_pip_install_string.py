import pytest

from fractal_server.app.models.v2 import TaskGroupV2


def test_pip_install_string():
    # No wheel path
    tg = TaskGroupV2(pkg_name="pkg", version="1.2.3")
    assert tg.pip_install_string == "pkg==1.2.3"
    tg = TaskGroupV2(pkg_name="pkg", version="1.2.3", pip_extras="extra1")
    assert tg.pip_install_string == "pkg[extra1]==1.2.3"
    tg = TaskGroupV2(
        pkg_name="pkg", version="1.2.3", pip_extras="extra1,extra2"
    )
    assert tg.pip_install_string == "pkg[extra1,extra2]==1.2.3"
    with pytest.raises(ValueError):
        # Fail because version=None
        tg = TaskGroupV2(pkg_name="pkg")
        tg.pip_install_string

    # Wheel path is set
    tg = TaskGroupV2(archive_path="/tmp/x.whl", pkg_name="pkg", version="1.2.3")
    assert tg.pip_install_string == "/tmp/x.whl"
    tg = TaskGroupV2(archive_path="/tmp/x.whl", pkg_name="pkg")
    assert tg.pip_install_string == "/tmp/x.whl"
    tg = TaskGroupV2(
        archive_path="/tmp/x.whl",
        pkg_name="pkg",
        version="1.2.3",
        pip_extras="extra1",
    )
    assert tg.pip_install_string == "/tmp/x.whl[extra1]"
    tg = TaskGroupV2(
        archive_path="/tmp/x.whl", pkg_name="pkg", pip_extras="extra1"
    )
    assert tg.pip_install_string == "/tmp/x.whl[extra1]"


def test_pinned_package_versions_string():
    tg = TaskGroupV2(pkg_name="pkg", version="1.2.3")
    assert tg.pinned_package_versions_pre_string == ""
    assert tg.pinned_package_versions_post_string == ""

    tg = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        pinned_package_versions_pre={},
        pinned_package_versions_post={},
    )
    assert tg.pinned_package_versions_pre_string == ""
    assert tg.pinned_package_versions_post_string == ""

    tg = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        pinned_package_versions_pre=dict(pkg1="v1", pkg2="v2"),
        pinned_package_versions_post=dict(A="1.2.3a", B="3.2.1b"),
    )
    assert tg.pinned_package_versions_pre_string == "pkg1==v1 pkg2==v2"
    assert tg.pinned_package_versions_post_string == "A==1.2.3a B==3.2.1b"


def test_properties_for_pixi_task_group():
    tg = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pixi",
    )
    with pytest.raises(ValueError):
        assert tg.pip_install_string
    with pytest.raises(ValueError):
        assert tg.pinned_package_versions_pre_string
    with pytest.raises(ValueError):
        assert tg.pinned_package_versions_post_string
