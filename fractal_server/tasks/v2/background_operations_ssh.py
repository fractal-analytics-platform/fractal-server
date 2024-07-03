import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric import Connection
from sqlalchemy.orm.attributes import flag_modified

from ...app.models.v2 import CollectionStateV2
from ._TaskCollectPip import _TaskCollectPip
from .background_operations import _handle_failure
from .background_operations import _insert_tasks
from .background_operations import _prepare_tasks_metadata
from .background_operations import _set_collection_state_data_status
from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import run_command_over_ssh
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils import get_python_interpreter_v2

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _parse_script_5_stdout(stdout: str) -> dict[str, str]:
    searches = [
        ("Python interpreter:", "python_bin"),
        ("Package name:", "package_name"),
        ("Package version:", "package_version"),
        ("Package parent folder:", "package_root_remote"),
        ("Manifest absolute path:", "manifest_path_remote"),
    ]
    stdout_lines = stdout.splitlines()
    attributes = dict()
    for search, attribute_name in searches:
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


def _customize_and_run_template(
    script_filename: str,
    templates_folder: Path,
    replacements: list[tuple[str, str]],
    tmpdir: str,
    logger_name: str,
    connection: Connection,
) -> str:
    """
    Customize one of the template bash scripts, transfer it to the remote host
    via SFTP and then run it via SSH.

    Args:
        script_filename:
        templates_folder:
        replacements:
        tmpdir:
        logger_name:
        connection:
    """
    logger = get_logger(logger_name)
    logger.info(f"Handling {script_filename} - START")
    settings = Inject(get_settings)

    # Read template
    template_path = templates_folder / script_filename
    with template_path.open("r") as f:
        script_contents = f.read()
    # Customize template
    for old_new in replacements:
        script_contents = script_contents.replace(old_new[0], old_new[1])
    # Write script locally
    script_path_local = Path(tmpdir) / script_filename
    with script_path_local.open("w") as f:
        f.write(script_contents)
    # Transfer script to remote host

    script_path_remote = os.path.join(
        settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR,
        f"script_{abs(hash(tmpdir))}{script_filename}",
    )
    logger.info(f"Now transfer {script_path_local=} to {script_path_remote=}")
    connection.put(
        local=script_path_local,
        remote=script_path_remote,
    )
    # Execute script remotely
    cmd = f"bash {script_path_remote}"
    logger.info(f"Now run {cmd=}")
    stdout = run_command_over_ssh(cmd=cmd, connection=connection)
    logger.info(stdout)
    logger.info(f"Handling {script_filename} - END")

    return stdout


async def background_collect_pip_ssh(
    state_id: int,
    task_pkg: _TaskCollectPip,
    connection: Connection,
) -> None:

    with TemporaryDirectory() as tmpdir:
        LOGGER_NAME = "task_collection_ssh"
        log_file_path = Path(tmpdir) / "log"
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        # Prepare replacements for task-collection scripts
        settings = Inject(get_settings)
        python_bin = get_python_interpreter_v2(version=task_pkg.python_version)
        package_version = (
            ""
            if task_pkg.package_version is None
            else task_pkg.package_version
        )

        install_string = task_pkg.package
        if task_pkg.package_extras is not None:
            install_string = f"{install_string}[{task_pkg.package_extras}]"
        if task_pkg.package_version is not None:
            install_string = f"{install_string}=={task_pkg.package_version}"
        package_env_dir = (
            Path(settings.FRACTAL_SLURM_SSH_WORKING_BASE_DIR)
            / ".fractal"
            / f"{task_pkg.package_name}{package_version}"
        ).as_posix()

        replacements = [
            ("__PACKAGE_NAME__", task_pkg.package_name),
            ("__PACKAGE_ENV_DIR__", package_env_dir),
            ("__PACKAGE__", task_pkg.package),
            ("__PYTHON__", python_bin),
            ("__INSTALL_STRING__", install_string),
        ]

        with next(get_sync_db()) as db:

            try:

                common_args = dict(
                    templates_folder=TEMPLATES_DIR,
                    replacements=replacements,
                    tmpdir=tmpdir,
                    logger_name=LOGGER_NAME,
                    connection=connection,
                )

                logger.debug("Task collection - installing - START")
                _set_collection_state_data_status(
                    state_id=state_id,
                    new_status="installing",
                    logger_name=LOGGER_NAME,
                    db=db,
                )
                stdout = _customize_and_run_template(
                    script_filename="_1_create_venv.sh",
                    **common_args,
                )
                stdout = _customize_and_run_template(
                    script_filename="_2_upgrade_pip.sh",
                    **common_args,
                )
                stdout = _customize_and_run_template(
                    script_filename="_3_pip_install.sh",
                    **common_args,
                )
                stdout_pip_freeze = _customize_and_run_template(
                    script_filename="_4_pip_freeze.sh",
                    **common_args,
                )
                logger.debug("Task collection - installing - END")

                logger.debug("Task collection - collecting - START")
                _set_collection_state_data_status(
                    state_id=state_id,
                    new_status="collecting",
                    logger_name=LOGGER_NAME,
                    db=db,
                )
                stdout = _customize_and_run_template(
                    script_filename="_5_pip_show.sh",
                    **common_args,
                )

                pkg_attrs = _parse_script_5_stdout(stdout)
                for key, value in pkg_attrs.items():
                    logger.debug(f"Parsed pip-show output: {key}={value}")
                # Check package_name match
                # FIXME SSH: Does this work for non-canonical `package_name`?
                package_name_pip_show = pkg_attrs.get("package_name")
                package_name_task_pkg = task_pkg.package_name
                if package_name_pip_show != package_name_task_pkg:
                    raise ValueError(
                        f"`package_name` mismatch: "
                        f"{package_name_task_pkg=} but "
                        f"{package_name_pip_show=}"
                    )
                # Extract/drop parsed attributes
                pkg_attrs.pop("package_name")
                python_bin = pkg_attrs.pop("python_bin")
                package_root_remote = pkg_attrs.pop("package_root_remote")
                manifest_path_remote = pkg_attrs.pop("manifest_path_remote")

                # Read and validate remote manifest file
                with connection.sftp().open(manifest_path_remote, "r") as f:
                    manifest = json.load(f)
                logger.info(f"Manifest loaded from {manifest_path_remote=}")
                ManifestV2(**manifest)
                logger.info("Manifest is valid ManifestV2 object.")

                # Create new _TaskCollectPip object
                new_pkg = _TaskCollectPip(
                    **task_pkg.dict(
                        exclude={"package_version", "package_name"},
                        exclude_unset=True,
                        exclude_none=True,
                    ),
                    package_manifest=manifest,
                    **pkg_attrs,
                )

                task_list = _prepare_tasks_metadata(
                    package_manifest=new_pkg.package_manifest,
                    package_version=new_pkg.package_version,
                    package_source=new_pkg.package_source,
                    package_root=Path(package_root_remote),
                    python_bin=Path(python_bin),
                )
                _insert_tasks(task_list=task_list, db=db)
                logger.debug("Task collection - collecting - END")

                # Finalize (write metadata to DB)
                logger.debug("Task collection - finalising - START")
                collection_state = db.get(CollectionStateV2, state_id)
                collection_state.data["log"] = log_file_path.open("r").read()
                collection_state.data["freeze"] = stdout_pip_freeze
                flag_modified(collection_state, "data")
                db.commit()
                logger.debug("Task collection - finalising - END")

            except Exception as e:
                _handle_failure(
                    state_id=state_id,
                    log_file_path=log_file_path,
                    logger_name=LOGGER_NAME,
                    exception=e,
                    db=db,
                )
                return
