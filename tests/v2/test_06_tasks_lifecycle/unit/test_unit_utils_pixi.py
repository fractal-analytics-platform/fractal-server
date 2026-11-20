import pytest

from fractal_server.tasks.v2.utils_pixi import parse_collect_stdout
from fractal_server.tasks.v2.utils_pixi import parse_stdout_value


def test_parse_stdout_value():
    stdout = "line 1\nPackage folder: /source/pkg\nline 3\n"
    assert parse_stdout_value(stdout, "Package folder") == "/source/pkg"

    with pytest.raises(ValueError, match="found 0 times"):
        parse_stdout_value("line 1\nline 2\n", "Package folder")

    with pytest.raises(ValueError, match="found 2 times"):
        parse_stdout_value(
            "Package folder: /a\nPackage folder: /b\n", "Package folder"
        )


def test_parse_collect_stdout():
    with pytest.raises(ValueError):
        parse_collect_stdout("Package folder: abc\nPackage folder: cde\n")

    with pytest.raises(ValueError):
        parse_collect_stdout("nothing interesting")

    text = (
        "Package folder: abc\n"
        "something else\n"
        "whatever\n"
        "Last line\n"
        "Project Python wrapper: whatever"
    )
    output = parse_collect_stdout(text)
    assert output == dict(package_root="abc", project_python_wrapper="whatever")
