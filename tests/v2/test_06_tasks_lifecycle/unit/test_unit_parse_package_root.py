import pytest

from fractal_server.tasks.v2.ssh.collect_apptainer import _parse_package_root


def test_parse_package_root():
    stdout = "line 1\nPackage folder: /source/pkg\nline 3\n"
    assert _parse_package_root(stdout) == "/source/pkg"

    with pytest.raises(ValueError, match="found 0 times"):
        _parse_package_root("line 1\nline 2\n")

    with pytest.raises(ValueError, match="found 2 times"):
        _parse_package_root("Package folder: /a\nPackage folder: /b\n")
