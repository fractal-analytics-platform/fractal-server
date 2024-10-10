import pytest

from fractal_server.app.models.v2 import TaskGroupV2


async def test_pip_install_string():
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
    tg = TaskGroupV2(wheel_path="/tmp/x.whl", pkg_name="pkg", version="1.2.3")
    assert tg.pip_install_string == "/tmp/x.whl"
    tg = TaskGroupV2(wheel_path="/tmp/x.whl", pkg_name="pkg")
    assert tg.pip_install_string == "/tmp/x.whl"
    tg = TaskGroupV2(
        wheel_path="/tmp/x.whl",
        pkg_name="pkg",
        version="1.2.3",
        pip_extras="extra1",
    )
    assert tg.pip_install_string == "/tmp/x.whl[extra1]"
    tg = TaskGroupV2(
        wheel_path="/tmp/x.whl", pkg_name="pkg", pip_extras="extra1"
    )
    assert tg.pip_install_string == "/tmp/x.whl[extra1]"
