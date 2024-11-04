import pytest

from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.tasks.v2.utils_templates import parse_script_5_stdout
from fractal_server.utils import execute_command_sync


def test_parse_script_5_stdout():
    stdout = (
        "Python interpreter: /some\n"
        "Package name: name\n"
        "Package version: version\n"
        "Package parent folder: /some\n"
        "Manifest absolute path: /some\n"
    )
    res = parse_script_5_stdout(stdout)
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
        parse_script_5_stdout(stdout)

    with pytest.raises(ValueError, match="not found"):
        parse_script_5_stdout("invalid")


def test_template_1(tmp_path, current_py_version):
    path = tmp_path / "unit_templates"
    venv_path = path / "venv"
    replacements = [
        ("__TASK_GROUP_DIR__", path.as_posix()),
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
    ]
    script_path = tmp_path / "1_good.sh"
    customize_template(
        template_name="_1_create_venv.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert venv_path.exists()

    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
    ]

    script_path = tmp_path / "1_bad_missing_path.sh"
    customize_template(
        template_name="_1_create_venv.sh",
        replacements=replacements,
        script_path=script_path,
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path}")
    assert "returncode=1" in str(expinfo.value)

    replacements = [
        ("__TASK_GROUP_DIR__", path.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
    ]

    script_path = tmp_path / "1_bad_missing_venv_path.sh"
    customize_template(
        template_name="_1_create_venv.sh",
        replacements=replacements,
        script_path=script_path,
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path}")
    assert "returncode=1" in str(expinfo.value)


def test_template_3(tmp_path, testdata_path, current_py_version):
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
        ("__TASK_GROUP_DIR__", path.as_posix()),
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
        ("__PINNED_PACKAGE_LIST__", pinned_pkg_list),
    ]
    script_path = tmp_path / "3_good.sh"
    customize_template(
        template_name="_3_pip_install.sh",
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
        ("__TASK_GROUP_DIR__", path.as_posix()),
        ("__PACKAGE_ENV_DIR__", venv_path_bad.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
        ("__PINNED_PACKAGE_LIST__", pinned_pkg_list),
    ]
    script_path = tmp_path / "3_bad_pkg.sh"
    customize_template(
        template_name="_3_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "Package(s) not found: pkgA" in str(expinfo.value)


def test_template_5(tmp_path, testdata_path, current_py_version):

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
        ("__TASK_GROUP_DIR__", path.as_posix()),
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
        ("__PACKAGE_NAME__", package_name),
    ]
    script_path = tmp_path / "5_good.sh"
    customize_template(
        template_name="_5_pip_show.sh",
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
        ("__TASK_GROUP_DIR__", path.as_posix()),
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__INSTALL_STRING__", install_string_miss.as_posix()),
        ("__PYTHON__", f"python{current_py_version}"),
        ("__PACKAGE_NAME__", package_name),
    ]
    script_path = tmp_path / "5_good.sh"
    customize_template(
        template_name="_5_pip_show.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "ERROR: manifest path not found" in str(expinfo.value)
