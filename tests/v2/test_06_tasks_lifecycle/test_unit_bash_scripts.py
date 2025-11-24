import pytest
from devtools import debug

from fractal_server.tasks.v2.local.collect import _customize_and_run_template
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.tasks.v2.utils_templates import parse_script_pip_show_stdout
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
    tmp_path, testdata_path, current_py_version, local_resource_profile_db
):
    resource, profile = local_resource_profile_db
    resource.tasks_python_config["pip_cache_dir"] = (
        tmp_path / "pip_cache"
    ).as_posix()

    path = tmp_path / "unit_templates"

    # Case 1: successful `pip install`, with pinned packages
    venv_path = path / "venv"
    install_string = testdata_path.parent / (
        "v2/fractal_tasks_valid/valid_tasks/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PINNED_PACKAGE_LIST_PRE__", "pydantic==2.12.0"),
        ("__PINNED_PACKAGE_LIST_POST__", "devtools==0.12.2"),
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", resource.pip_cache_dir_arg),
    ]
    script_path = tmp_path / "2_good.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    stdout = execute_command_sync(command=f"bash {script_path.as_posix()}")
    debug(stdout)
    assert "pydantic-2.12.0" in stdout
    assert "Successfully installed fractal-tasks-mock-0.0.1" in stdout
    assert "devtools-0.12.2" in stdout

    # Case 2: successfull `pip show`
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

    # Case 3: Failed `pip install`, due to invalid `pinned_pkg_list`
    venv_path_bad = path / "bad_venv"
    pinned_pkg_list_post = "non_existing_package==1.2.3"
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path_bad}"
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path_bad.as_posix()),
        ("__INSTALL_STRING__", install_string.as_posix()),
        ("__PINNED_PACKAGE_LIST_PRE__", ""),
        ("__PINNED_PACKAGE_LIST_POST__", pinned_pkg_list_post),
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", resource.pip_cache_dir_arg),
    ]
    script_path = tmp_path / "2_bad_pkg.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as e_info:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    ERROR_MSG = (
        "Could not find a version that satisfies the requirement "
        f"{pinned_pkg_list_post}"
    )
    assert ERROR_MSG in str(e_info.value)

    # Case 4: Failed `pip install`, due to invalid wheel name
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
        ("__PINNED_PACKAGE_LIST_PRE__", ""),
        ("__PINNED_PACKAGE_LIST_POST__", ""),
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", resource.pip_cache_dir_arg),
    ]
    script_path = tmp_path / "2_bad_whl.sh"
    customize_template(
        template_name="2_pip_install.sh",
        replacements=replacements,
        script_path=script_path.as_posix(),
    )
    with pytest.raises(RuntimeError) as e_info:
        execute_command_sync(command=f"bash {script_path.as_posix()}")
    # We make the assertion flexible, since the error message changed with
    # pip 25.1
    condition_1 = (
        "ERROR: fractal_tasks_mock-0.0.1-py3-none-any (2).whl"
        " is not a valid wheel filename"
    ) in str(e_info.value)
    condition_2 = (
        "ERROR: fractal_tasks_mock-0.0.1-py3-none-any (2).whl"
        " is not a supported wheel on this platform"
    ) in str(e_info.value)
    assert condition_1 or condition_2


def test_template_4_missing_manifest(tmp_path, current_py_version):
    venv_path = (tmp_path / "venv_miss_manifest").as_posix()
    execute_command_sync(
        command=f"python{current_py_version} -m venv {venv_path}"
    )
    replacements = [
        ("__PACKAGE_ENV_DIR__", venv_path),
        ("__PACKAGE_NAME__", "pip"),
    ]
    script_path = (tmp_path / "4_good.sh").as_posix()
    customize_template(
        template_name="4_pip_show.sh",
        replacements=replacements,
        script_path=script_path,
    )
    with pytest.raises(RuntimeError) as expinfo:
        execute_command_sync(command=f"bash {script_path}")
    assert "ERROR: manifest path not found" in str(expinfo.value)


def _parse_pip_freeze_output(stdout: str) -> dict[str, str]:
    splitted_output = stdout.split()
    freeze_dict = dict([x.split("==") for x in splitted_output])
    return freeze_dict


def test_templates_freeze(
    tmp_path, current_py_version, local_resource_profile_db
):
    resource, profile = local_resource_profile_db

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

    # Pip-install devtools on 'venv1'
    _customize_and_run_template(
        template_filename="2_pip_install.sh",
        replacements=[
            ("__PACKAGE_ENV_DIR__", venv_path_1.as_posix()),
            ("__INSTALL_STRING__", "devtools"),
            ("__PINNED_PACKAGE_LIST_PRE__", ""),
            ("__PINNED_PACKAGE_LIST_POST__", ""),
            ("__FRACTAL_PIP_CACHE_DIR_ARG__", resource.pip_cache_dir_arg),
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
    assert "devtools" in dependencies_1

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
            ("__FRACTAL_PIP_CACHE_DIR_ARG__", resource.pip_cache_dir_arg),
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
    size_in_kB, file_number = (int(item) for item in stdout.split())
    print(f"{size_in_kB=}")
    print(f"{file_number=}")

    # Check that file number and (approximate) disk usage
    assert file_number == len(FILES)
    EXPECTED_SIZE_IN_KB = FILESIZE_IN_MB * 1024 * len(FILES)
    assert abs(size_in_kB - EXPECTED_SIZE_IN_KB) < 0.02 * size_in_kB
