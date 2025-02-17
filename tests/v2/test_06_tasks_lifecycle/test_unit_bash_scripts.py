import pytest

from fractal_server.config import Settings
from fractal_server.tasks.v2.local.collect import _customize_and_run_template
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


def test_template_2(
    tmp_path, testdata_path, current_py_version, override_settings_factory
):
    settings = Settings(
        **Settings(
            FRACTAL_PIP_CACHE_DIR=(tmp_path / "CACHE_DIR").as_posix()
        ).model_dump(exclude_unset=True)
    )
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
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", settings.PIP_CACHE_DIR_ARG),
    ]
    script_path = tmp_path / "2_good.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    stdout = execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "installing pinned versions fractal-tasks-mock==0.0.1" in stdout

    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__PACKAGE_NAME__", "fractal-tasks-mock"),
    ]
    script_path = tmp_path / "4_good.sh"
    customize_template(
        template_name="4_pip_show.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    stdout = execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert "OK: manifest path exists" in stdout

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
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", Settings().PIP_CACHE_DIR_ARG),
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

    venv_path = path / "bad_wheel"
    install_string = testdata_path.parent / (
        "v2/fractal_tasks_valid/valid_tasks/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any (2).whl"
    )
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__FRACTAL_MAX_PIP_VERSION__", "99"),
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", settings.PIP_CACHE_DIR_ARG),
    ]
    script_path = tmp_path / "2_bad_whl.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    assert (
        "ERROR: fractal_tasks_mock-0.0.1-py3-none-any (2).whl"
        " is not a valid wheel filename"
    ) in str(expinfo.value)


def test_template_4_missing_manifest(
    tmp_path, testdata_path, current_py_version
):

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
    splitted_output = stdout.split()
    freeze_dict = dict([x.split("==") for x in splitted_output])
    return freeze_dict


def test_templates_freeze(tmp_path, current_py_version):
    # Create two venvs
    venv_path_1 = tmp_path / "venv1"
    venv_path_2 = tmp_path / "venv2"
    for venv_path in [venv_path_1, venv_path_2]:
        _customize_and_run_template(
            template_filename="1_create_venv.sh",
            replacements=[
                ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
                ("__PYTHON__", f"python{current_py_version}"),
            ],
            script_dir=tmp_path,
            logger_name=__name__,
            prefix="prefix",
        )
        _customize_and_run_template(
            template_filename="2_pip_install.sh",
            replacements=[
                ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
                ("__INSTALL_STRING__", "pip"),
                ("__FRACTAL_MAX_PIP_VERSION__", "99"),
                ("__PINNED_PACKAGE_LIST__", ""),
                (
                    "__FRACTAL_PIP_CACHE_DIR_ARG__",
                    Settings().PIP_CACHE_DIR_ARG,
                ),
            ],
            script_dir=tmp_path,
            logger_name=__name__,
            prefix="prefix",
        )

    # Pip-install devtools on 'venv1'
    _customize_and_run_template(
        template_filename="2_pip_install.sh",
        replacements=[
            ("__PACKAGE_ENV_DIR__", venv_path_1.as_posix()),
            ("__INSTALL_STRING__", "devtools"),
            ("__FRACTAL_MAX_PIP_VERSION__", "99"),
            ("__PINNED_PACKAGE_LIST__", ""),
            ("__FRACTAL_PIP_CACHE_DIR_ARG__", Settings().PIP_CACHE_DIR_ARG),
        ],
        script_dir=tmp_path,
        logger_name=__name__,
        prefix="prefix",
    )
    # Run script 3 (pip freeze) on 'venv1'
    pip_freeze_venv_1 = _customize_and_run_template(
        template_filename="3_pip_freeze.sh",
        replacements=[("__PACKAGE_ENV_DIR__", venv_path_1.as_posix())],
        script_dir=tmp_path,
        logger_name=__name__,
        prefix="prefix",
    )
    dependencies_1 = _parse_pip_freeze_output(pip_freeze_venv_1)
    assert "pip" in dependencies_1

    # Write requirements file
    requirements_file = tmp_path / "requirements.txt"
    with requirements_file.open("w") as f:
        f.write(pip_freeze_venv_1)

    # Run script 6 (install from freeze) on 'venv2'
    _customize_and_run_template(
        template_filename="6_pip_install_from_freeze.sh",
        replacements=[
            ("__PACKAGE_ENV_DIR__", venv_path_2.as_posix()),
            ("__PIP_FREEZE_FILE__", requirements_file.as_posix()),
            ("__FRACTAL_MAX_PIP_VERSION__", "99"),
            ("__FRACTAL_PIP_CACHE_DIR_ARG__", Settings().PIP_CACHE_DIR_ARG),
        ],
        script_dir=tmp_path,
        logger_name=__name__,
        prefix="prefix",
    )

    # Run script 3 (pip freeze) on 'venv2'
    pip_freeze_venv_2 = _customize_and_run_template(
        template_filename="3_pip_freeze.sh",
        replacements=[("__PACKAGE_ENV_DIR__", venv_path_2.as_posix())],
        script_dir=tmp_path,
        logger_name=__name__,
        prefix="prefix",
    )
    dependencies_2 = _parse_pip_freeze_output(pip_freeze_venv_2)

    assert dependencies_2 == dependencies_1


def test_venv_size_and_file_number(tmp_path):
    # Create folders
    folder = tmp_path / "test"
    subfolder = folder / "subfolder"
    subfolder.mkdir(parents=True)

    # Create files
    FILESIZE_IN_MB = 1
    FILES = [folder / "file1", folder / "file2", subfolder / "file3"]
    for file in FILES:
        with file.open("wb") as f:
            f.write(b"_" * FILESIZE_IN_MB * (1024**2))

    # Run script
    stdout = _customize_and_run_template(
        template_filename="5_get_venv_size_and_file_number.sh",
        replacements=[("__PACKAGE_ENV_DIR__", folder.as_posix())],
        script_dir=tmp_path,
        logger_name=__name__,
        prefix="prefix",
    )
    size_in_kB, file_number = [int(item) for item in stdout.split()]
    print(f"{size_in_kB=}")
    print(f"{file_number=}")

    # Check that file number and (approximate) disk usage
    assert file_number == len(FILES)
    EXPECTED_SIZE_IN_KB = FILESIZE_IN_MB * 1024 * len(FILES)
    assert abs(size_in_kB - EXPECTED_SIZE_IN_KB) < 0.02 * size_in_kB
