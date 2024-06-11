import json
import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric import Connection
from invoke import UnexpectedExit
from paramiko.ssh_exception import NoValidConnectionsError
from sqlalchemy.orm.attributes import flag_modified

from ...app.models.v2 import CollectionStateV2
from ..utils import _normalize_package_name
from ..utils import slugify_task_name
from ._TaskCollectPip import _TaskCollectPip
from .background_operations import _insert_tasks
from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

logger = set_logger(__name__)


def _run_command_over_ssh(
    *,
    cmd: str,
    connection: Connection,
) -> str:
    """
    Run a command within an open SSH connection.

    FIXME: this is duplicated from other parts of the codebase.

    Args:
        cmd: Command to be run
        connection: Fabric connection object

    Returns:
        Standard output of the command, if successful.
    """
    t_0 = time.perf_counter()
    logger.info(f"START running '{cmd}' over SSH.")
    try:
        res = connection.run(cmd, hide=True)
    except UnexpectedExit as e:
        error_msg = (
            f"Running command `{cmd}` over SSH failed.\n"
            f"Original error:\n{str(e)}."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    except NoValidConnectionsError as e:
        error_msg = (
            f"Running command `{cmd}` over SSH failed.\n"
            f"Original NoValidConnectionError:\n{str(e)}.\n"
            f"{e.errors=}\n"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    t_1 = time.perf_counter()
    logger.info(f"END   running '{cmd}' over SSH, elapsed {t_1-t_0:.3f}")
    if res.stdout:
        logger.info(f"STDOUT:\n{res.stdout}")
    if res.stderr:
        logger.info(f"STDERR:\n{res.stderr}")
    return res.stdout


async def _add_tasks_to_db(
    *,
    task_pkg: _TaskCollectPip,
    python_bin: str,
    package_parent_folder_remote: str,
    connection: Connection,
) -> list[TaskCreateV2]:
    """
    Based on a _TaskCollectPip object some additional parameters, create a
    list of tasks to be added to the DB.
    """

    # Normalize package name
    task_pkg.package_name = _normalize_package_name(task_pkg.package_name)
    task_pkg.package = _normalize_package_name(task_pkg.package)

    # Only proceed if package, version and manifest attributes are set
    # task_pkg.check()  # FIXME: would this work? To be verified.

    package_root = Path(
        package_parent_folder_remote, task_pkg.package.replace("-", "_")
    )

    try:
        logger.debug("[create_task_list] START")
        task_list = []
        for t in task_pkg.package_manifest.task_list:
            logger.debug(f"[create_task_list] Now handling task '{t.name}'")

            # Fill in attributes for TaskCreate
            task_attributes = {}
            task_attributes["version"] = task_pkg.package_version
            task_name_slug = slugify_task_name(t.name)
            task_attributes[
                "source"
            ] = f"{task_pkg.package_source}:{task_name_slug}"
            # Executables
            for ind, executable in enumerate(
                [t.executable_non_parallel, t.executable_parallel]
            ):
                if executable is None:
                    continue

                full_path = (package_root / executable).as_posix()
                try:
                    connection.sftp().stat(full_path)
                except Exception as e:
                    error_msg = (
                        f"An error occurred for excutable `{full_path}` "
                        f"(task `{t.name}`)."
                        f"Original error: {str(e)}"
                    )
                    raise ValueError(error_msg)

                if ind == 0:
                    task_attributes[
                        "command_non_parallel"
                    ] = f"{python_bin} {full_path}"
                else:
                    task_attributes[
                        "command_parallel"
                    ] = f"{python_bin} {full_path}"

            manifest = task_pkg.package_manifest
            if manifest.has_args_schemas:
                task_attributes[
                    "args_schema_version"
                ] = manifest.args_schema_version

            this_task = TaskCreateV2(
                **t.dict(
                    exclude={"executable_non_parallel", "executable_parallel"}
                ),
                **task_attributes,
            )
            task_list.append(this_task)
        logger.debug("[create_task_list] END")
    except Exception as e:
        logger.debug(f"[create_task_list] ERROR. Original error:\n{str(e)}")
        raise e

    logger.debug("[add_tasks_to_db] START")
    with next(get_sync_db()) as db_sync:
        await _insert_tasks(task_list=task_list, db=db_sync)
    logger.debug("[add_tasks_to_db] END")

    return task_list


def _parse_script_5_stdout(stdout: str) -> dict[str, str]:
    searches = [
        ("Python interpreter:", "python_bin"),
        ("Package name:", "package_name"),
        ("Package version:", "package_version"),
        ("Package parent folder:", "package_parent_folder_remote"),
        ("Manifest absolute path:", "manifest_absolute_path_remote"),
    ]
    stdout_lines = stdout.splitlines()
    attributes = dict()
    for (search, attribute_name) in searches:

        matching_lines = [_line for _line in stdout_lines if search in _line]
        if len(matching_lines) == 0:
            raise ValueError(f"String '{search}' not found in stdout.")
        elif len(matching_lines) > 1:
            raise ValueError(
                f"String '{search}' found too many times "
                f"({len(matching_lines)})."
            )
        else:
            actual_line = matching_lines[0]
            attribute_value = actual_line.split(search)[-1].strip(" ")
            attributes[attribute_name] = attribute_value
    return attributes


async def background_collect_pip_ssh(
    task_pkg: _TaskCollectPip, state_id: int, connection: Connection
) -> None:

    # Prepare replacements for task-collection scripts
    settings = Inject(get_settings)
    version_string = (
        f"=={task_pkg.package_version}" if task_pkg.package_version else ""
    )
    extras = f"[{task_pkg.package_extras}]" if task_pkg.package_extras else ""
    replacements = [
        ("__PYTHON__", settings.FRACTAL_SLURM_WORKER_PYTHON),
        ("__FRACTAL_TASKS_DIR__", settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR),
        ("__PACKAGE__", task_pkg.package),
        ("__VERSION__", version_string),
        ("__EXTRAS__", extras),
    ]

    # Load templates
    templates_folder = Path(__file__).parent / "templates"
    list_script_filenames = [
        "_1_create_main_folder.sh",
        "_2_create_venv.sh",
        "_3_upgrade_pip.sh",
        "_4_install_package.sh",
        "_5_extract_info.sh",
    ]

    with TemporaryDirectory() as tmpdir:

        with next(get_sync_db()) as db_sync:
            state = db_sync.get(CollectionStateV2, state_id)
            state.data["status"] = "installing"
            flag_modified(state, "data")
            db_sync.add(state)
            db_sync.commit()
            db_sync.refresh(state)

            for ind, script_filename in enumerate(list_script_filenames):
                logger.info(f"Handling {script_filename} - START")
                # Read template
                template_path = templates_folder / script_filename
                with template_path.open("r") as f:
                    script_contents = f.read()
                # Customize template
                for old_new in replacements:
                    script_contents = script_contents.replace(
                        old_new[0], old_new[1]
                    )
                # Write script locally
                script_path_local = Path(tmpdir) / f"script_{ind}.sh"
                with script_path_local.open("w") as f:
                    f.write(script_contents)
                # Transfer script to remote host
                script_path_remote = os.path.join(
                    settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR,
                    f"script_{hash(tmpdir)}{script_filename}",
                )
                connection.put(
                    local=script_path_local,
                    remote=script_path_remote,
                )
                # Execute script remotely
                try:
                    stdout = _run_command_over_ssh(
                        cmd=f"bash {script_path_remote}", connection=connection
                    )
                except Exception as e:
                    error_msg = (
                        f"ERROR while running {script_path_remote}.\n"
                        f"Original error: {str(e)}"
                    )
                    logger.error(error_msg)
                    old_log = state.data.get("log", None) or ""
                    new_log = f"{old_log}\n{error_msg}"
                    state.data["log"] = new_log
                    state.data["status"] = "fail"
                    flag_modified(state, "data")
                    db_sync.add(state)
                    db_sync.commit()
                    return

                old_log = state.data.get("log", None) or ""
                new_log = f"{old_log}\n{stdout}\n"
                state.data["log"] = new_log
                flag_modified(state, "data")
                db_sync.add(state)
                db_sync.commit()
                db_sync.refresh(state)
                logger.info(f"Handling {script_filename} - END")

    pkg_attrs = _parse_script_5_stdout(stdout)
    python_bin = pkg_attrs.pop("python_bin")
    package_parent_folder_remote = pkg_attrs.pop(
        "package_parent_folder_remote"
    )
    manifest_absolute_path_remote = pkg_attrs.pop(
        "manifest_absolute_path_remote"
    )

    # Read remote manifest file
    with connection.sftp().open(manifest_absolute_path_remote, "r") as f:
        manifest = json.load(f)
    logger.info(f"I loaded the manifest from {manifest_absolute_path_remote}")

    # Create new _TaskCollectPip object
    new_pkg = _TaskCollectPip(
        **task_pkg.dict(
            exclude={"package_version", "python_version"},
            exclude_unset=True,
            exclude_none=True,
        ),
        package_manifest=manifest,
        python_version="N/A",
        **pkg_attrs,
    )

    # Create list of TaskCreateV2 objects and insert them into the database
    task_list = await _add_tasks_to_db(
        task_pkg=new_pkg,
        python_bin=python_bin,
        package_parent_folder_remote=package_parent_folder_remote,
        connection=connection,
    )
    task_list

    # TODO: add here logic to handle the state
    with next(get_sync_db()) as db_sync:
        state = db_sync.get(CollectionStateV2, state_id)
        old_log = state.data.get("log", None) or ""
        new_log = f"{old_log}\n{stdout}\n"
        state.data["log"] = new_log
        state.data["status"] = "OK"
        flag_modified(state, "data")
        db_sync.add(state)
        db_sync.commit()
