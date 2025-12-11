import pytest

from fractal_server.tasks.v2.utils_pixi import parse_collect_stdout


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
