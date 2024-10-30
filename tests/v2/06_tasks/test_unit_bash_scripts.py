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


# async def test_pip_install_with_pinned_dependencies(tmp_path, caplog):
#     settings = Inject(get_settings)
#     PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
#     pkg_path = tmp_path / "devtools/0.8.0/"
#     EXTRA = "pygments"

#     LOGGER_NAME = "pinned_dependencies"
#     logger = set_logger(LOGGER_NAME)
#     logger.propagate = True

#     # Create TaskGroupCreateV2 (to mimic TaskGroupV2 object)
#     common_attrs = dict(
#         pkg_name="devtools",
#         version="0.8.0",
#         wheel_path=None,
#         python_version=PYTHON_VERSION,
#         user_id=0,
#         origin="pypi",
#         path=pkg_path.as_posix(),
#         venv_path=(pkg_path / "venv").as_posix(),
#         pip_extras=EXTRA,
#     )
#     task_group = TaskGroupV2(**common_attrs)

#     # Prepare venv
#     Path(task_group.venv_path).mkdir(exist_ok=True, parents=True)
#     await _init_venv_v2(
#         venv_path=Path(task_group.venv_path),
#         python_version=task_group.python_version,
#         logger_name=LOGGER_NAME,
#     )

#     async def _aux(_task_group) -> str:
#         """pip install with pin and return version for EXTRA package"""
#         await _pip_install(
#             task_group=_task_group,
#             logger_name=LOGGER_NAME,
#         )

#         # Check freeze file
#         freeze_file = Path(_task_group.path) / COLLECTION_FREEZE_FILENAME
#         assert freeze_file.exists()
#         with freeze_file.open("r") as f:
#             freeze_data = f.read().splitlines()
#         debug(freeze_data)
#         extra_version = next(
#             line.split("==")[1]
#             for line in freeze_data
#             if line.lower().startswith(EXTRA.lower())
#         )
#         debug(extra_version)
#         # Clean up
#         python_bin = (Path(task_group.venv_path) / "bin/python").as_posix()
#         await execute_command(
#             f"{python_bin} -m pip uninstall {task_group.pkg_name} {EXTRA} -y"
#         )
#         return extra_version

#     # Case 0:
#     #   get default version of EXTRA, and then use it as a pin
#     DEFAULT_VERSION = await _aux(TaskGroupV2(**common_attrs))
#     caplog.clear()
#     pin = {EXTRA: DEFAULT_VERSION}
#     new_version = await _aux(
#         TaskGroupV2(**common_attrs, pinned_package_versions=pin)
#     )
#     assert new_version == DEFAULT_VERSION
#     assert "Specific version required" in caplog.text
#     assert "already matches the pinned version" in caplog.text
#     caplog.clear()

#     # Case 1: pin EXTRA to a specific (non-default) version
#     PIN_VERSION = "2.0"
#     assert PIN_VERSION != DEFAULT_VERSION
#     pin = {EXTRA: PIN_VERSION}
#     new_version = await _aux(
#         TaskGroupV2(**common_attrs, pinned_package_versions=pin)
#     )

#     assert new_version == PIN_VERSION
#     assert "differs from pinned version" in caplog.text
#     assert f"pip install {EXTRA}" in caplog.text
#     caplog.clear()

#     # Case 2: bad pin with invalid EXTRA version
#     INVALID_EXTRA_VERSION = "123456789"
#     pin = {EXTRA: INVALID_EXTRA_VERSION}
#     with pytest.raises(RuntimeError) as error_info:
#         await _aux(TaskGroupV2(**common_attrs, pinned_package_versions=pin))
#     assert f"pip install {EXTRA}=={INVALID_EXTRA_VERSION}" in caplog.text
#     assert (
#         "Could not find a version that satisfies the requirement "
#         f"{EXTRA}=={INVALID_EXTRA_VERSION}"
#     ) in str(error_info.value)
#     caplog.clear()

#     # Case 3: bad pin with package which was not already installed
#     pin = {"pydantic": "1.0.0"}
#     with pytest.raises(RuntimeError) as error_info:
#         await _aux(TaskGroupV2(**common_attrs, pinned_package_versions=pin))
#     assert "pip show pydantic" in caplog.text
#     assert "Package(s) not found: pydantic" in str(error_info.value)
#     caplog.clear()
