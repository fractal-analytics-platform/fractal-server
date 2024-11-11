import pytest

from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.tasks.v2.utils_templates import (
    parse_script_pip_show_stdout,
)
from fractal_server.utils import execute_command_sync


def test_parse_script_pip_show_stdout():
    stdout = (
        "Python interpreter: /some\n"
        "Package name: name\n"
        "Package version: version\n"
        "Package parent folder: /some\n"
        "Manifest absolute path: /some\n"
    )
    res = parse_script_pip_show_stdout(stdout)
    assert res == {
        "python_bin": "/some",
        "package_name": "name",
        "package_version": "version",
        "package_root_parent": "/some",
        "manifest_path": "/some",
    }

    stdout = (
        "Python interpreter: /some\n"
        "Package name: name1\n"
        "Package name: name2\n"
        "Package version: version\n"
        "Package parent folder: /some\n"
        "Manifest absolute path: /some\n"
    )
    with pytest.raises(ValueError, match="too many times"):
        parse_script_pip_show_stdout(stdout)

    with pytest.raises(ValueError, match="not found"):
        parse_script_pip_show_stdout("invalid")


def test_template_1(tmp_path, current_py_version):
    path = tmp_path / "unit_templates"
    venv_path = path / "venv"
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
    ]
    script_path = tmp_path / "1_good.sh"
    customize_template(
        template_name="1_create_venv.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert venv_path.exists()


def test_template_2(tmp_path, testdata_path, current_py_version):
    path = tmp_path / "unit_templates"
    venv_path = path / "venv"
    install_string = testdata_path.parent / (
        "v2/fractal_tasks_valid/valid_tasks/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    pinned_pkg_list = "fractal-tasks-mock==0.0.1"
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PINNED_PACKAGE_LIST__", pinned_pkg_list),
        ("__FRACTAL_MAX_PIP_VERSION__", "99"),
    ]
    script_path = tmp_path / "2_good.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    stdout = execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "installing pinned versions fractal-tasks-mock==0.0.1" in stdout

    # create a wrong pinned_pkg_list
    venv_path_bad = path / "bad_venv"
    pinned_pkg_list = "pkgA==0.1.0 "

    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path_bad}"
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path_bad.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PINNED_PACKAGE_LIST__", pinned_pkg_list),
        ("__FRACTAL_MAX_PIP_VERSION__", "25"),
    ]
    script_path = tmp_path / "2_bad_pkg.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "Package(s) not found: pkgA" in str(expinfo.value)


def test_template_3(tmp_path, current_py_version):

    venv_path = tmp_path / "venv"
    replacements = [("__PACKAGE_ENV_DIR__", venv_path.as_posix())]

    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )

    script_path = tmp_path / "3_empty.sh"
    customize_template(
        template_name="3_pip_freeze.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    empty_stdout = execute_command_sync(
        command=f"bash {script_path.as_posix()}"
    )
    assert len(empty_stdout.split("\n")[:-1]) == 2

    execute_command_sync(command=f"{venv_path}/bin/pip install devtools")

    script_path = tmp_path / "3_devtools.sh"
    customize_template(
        template_name="3_pip_freeze.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )

    stdout = execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert len(stdout.split("\n")[:-1]) > 2


def test_template_4(tmp_path, testdata_path, current_py_version):

    path = tmp_path / "unit_templates"
    venv_path = path / "venv"
    install_string = testdata_path.parent / (
        "v2/fractal_tasks_valid/valid_tasks/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    package_name = "fractal-tasks-mock"
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )
    execute_command_sync(
        command=(
            f"{venv_path}/bin/python{current_py_version} "
            f"-m pip install {install_string}"
        )
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__PACKAGE_NAME__", package_name),
    ]
    script_path = tmp_path / "4_good.sh"
    customize_template(
        template_name="4_pip_show.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    stdout = execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "OK: manifest path exists" in stdout

    # use a package with missing manifest
    path = tmp_path / "unit_templates"
    venv_path = path / "venv_miss_manifest"
    install_string_miss = testdata_path.parent / (
        "v2/fractal_tasks_fail/missing_manifest/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    package_name = "fractal-tasks-mock"
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )
    execute_command_sync(
        command=(
            f"{venv_path}/bin/python{current_py_version} "
            f"-m pip install {install_string_miss}"
        )
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__PACKAGE_NAME__", package_name),
    ]
    script_path = tmp_path / "4_good.sh"
    customize_template(
        template_name="4_pip_show.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "ERROR: manifest path not found" in str(expinfo.value)
