import pytest
from devtools import debug

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


def _parse_pip_freeze_output(stdout: str) -> dict[str, str]:
    splitted_output = stdout.split("\n")[:-1]
    freeze_dict = {x.split("==")[0]: x.split("==")[1] for x in splitted_output}
    return freeze_dict


def test_template_3_and_5(tmp_path, current_py_version):

    # Create 'venv1'
    venv_path_1 = tmp_path / "venv1"
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path_1}"
    )

    # Run script 3 (pip freeze) on 'venv1'
    pip_freeze_script_path_1 = tmp_path / "pip_freeze.sh"
    customize_template(
        template_name="3_pip_freeze.sh",
        replacements=[("__PACKAGE_ENV_DIR__", venv_path_1.as_posix())],
        script_path=pip_freeze_script_path_1.as_posix(),
    )
    stdout_0 = execute_command_sync(
        command=f"bash {pip_freeze_script_path_1.as_posix()}"
    )
    dependencies_0 = _parse_pip_freeze_output(stdout_0)
    # Assert only
    assert len(dependencies_0) == 2
    assert "pip" in dependencies_0
    assert "setuptools" in dependencies_0

    # Pip-install devtools (on 'venv1')
    execute_command_sync(command=f"{venv_path_1}/bin/pip install devtools")
    stdout_1 = execute_command_sync(
        command=f"bash {pip_freeze_script_path_1.as_posix()}"
    )
    dependencies_1 = _parse_pip_freeze_output(stdout_1)
    assert dependencies_0.items() < dependencies_1.items()

    # Write requirements file
    requirements_file = tmp_path / "requirements.txt"
    with requirements_file.open("w") as f:
        f.write(stdout_1)

    # Create 'venv2'
    venv_path_2 = tmp_path / "venv2"
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path_2}"
    )

    # Run script 3 (pip freeze) on 'venv2'
    pip_freeze_script_path_2 = tmp_path / "pip_freeze_2.sh"
    customize_template(
        template_name="3_pip_freeze.sh",
        replacements=[("__PACKAGE_ENV_DIR__", venv_path_2.as_posix())],
        script_path=pip_freeze_script_path_2.as_posix(),
    )
    stdout_2 = execute_command_sync(
        command=f"bash {pip_freeze_script_path_2.as_posix()}"
    )
    dependencies_2 = _parse_pip_freeze_output(stdout_2)
    assert dependencies_2 == dependencies_0

    # Run script 5 (install from freeze) on 'venv2'

    pip_install_script = tmp_path / "install_from_freeze.sh"
    customize_template(
        template_name="5_pip_install_from_freeze.sh",
        replacements=[
            ("__PACKAGE_ENV_DIR__", venv_path_2.as_posix()),
            ("__PIP_FREEZE_FILE__", requirements_file.as_posix()),
            ("__FRACTAL_MAX_PIP_VERSION__", "99"),
        ],
        script_path=pip_install_script.as_posix(),
    )
    script_5_stdout = execute_command_sync(
        command=f"bash {pip_install_script.as_posix()}"
    )
    debug(script_5_stdout)

    stdout_3 = execute_command_sync(
        command=f"bash {pip_freeze_script_path_2.as_posix()}"
    )
    dependencies_3 = _parse_pip_freeze_output(stdout_3)
    assert dependencies_3 == dependencies_1
