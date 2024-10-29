import json
from pathlib import Path

from sqlalchemy.orm.attributes import flag_modified

from .background_operations import _handle_failure
from .background_operations import _prepare_tasks_metadata
from .background_operations import _set_collection_state_data_status
from .database_operations import create_db_tasks_and_update_task_group
from .template_utils import customize_template
from .template_utils import parse_script_5_stdout
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils import compare_package_names
from fractal_server.tasks.v2.utils import get_python_interpreter_v2
from fractal_server.utils import execute_command_sync


def _customize_and_run_template(
    script_filename: str,
    replacements: list[tuple[str, str]],
    script_dir: str,
    logger_name: str,
) -> str:
    """
    Customize one of the template bash scripts.

    Args:
        script_filename:
        replacements:
        script_dir:
        logger_name:
    """
    logger = get_logger(logger_name)
    logger.debug(f"_customize_and_run_template {script_filename} - START")

    script_path_local = Path(script_dir) / script_filename
    # Read template
    customize_template(
        template_name=script_filename,
        replacements=replacements,
        script_path=script_path_local,
    )

    cmd = f"bash {script_path_local}"
    logger.debug(f"Now run '{cmd}' ")

    stdout = execute_command_sync(command=cmd)

    logger.debug(f"Standard output of '{cmd}':\n{stdout}")
    logger.debug(f"_customize_and_run_template {script_filename} - END")

    return stdout


async def background_collect_pip_local(
    *,
    state_id: int,
    task_group: TaskGroupV2,
) -> None:
    """
    Collect a task package.

    This function is run as a background task, therefore exceptions must be
    handled.

    NOTE: by making this function sync, it will run within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Arguments:
        state_id:
        task_group:
    """

    # Check that the task_group path already exists
    if Path(task_group.path).exists():
        raise FileExistsError(f"{task_group.path} already exists.")
    # Create the task_group path
    Path(task_group.path).mkdir(parents=True)
    LOGGER_NAME = "task_collection_local"
    log_file_path = get_log_path(Path(task_group.path))
    logger = set_logger(
        logger_name=LOGGER_NAME,
        log_file_path=log_file_path,
    )
    logger.debug("START")
    for key, value in task_group.model_dump().items():
        logger.debug(f"task_group.{key}: {value}")

        # Open a DB session soon, since it is needed for updating `state`
    with next(get_sync_db()) as db:
        try:
            # Prepare replacements for task-collection scripts
            python_bin = get_python_interpreter_v2(
                python_version=task_group.python_version
            )
            install_string = task_group.pip_install_string
            settings = Inject(get_settings)
            replacements = [
                ("__PACKAGE_NAME__", task_group.pkg_name),
                ("__TASK_GROUP_DIR__", task_group.path),
                ("__PACKAGE_ENV_DIR__", task_group.venv_path),
                ("__PYTHON__", python_bin),
                ("__INSTALL_STRING__", install_string),
                (
                    "__FRACTAL_MAX_PIP_VERSION__",
                    settings.FRACTAL_MAX_PIP_VERSION,
                ),
            ]

            common_args = dict(
                replacements=replacements,
                script_dir=task_group.path,
                logger_name=LOGGER_NAME,
            )

            logger.debug("installing - START")
            _set_collection_state_data_status(
                state_id=state_id,
                new_status=CollectionStatusV2.INSTALLING,
                logger_name=LOGGER_NAME,
                db=db,
            )
            # Avoid keeping the db session open as we start some possibly
            # long operations that do not use the db
            db.close()

            logger.debug(
                (f"START - Create python venv {task_group.venv_path}")
            )
            cmd = (
                f"python{task_group.python_version} -m venv "
                f"{task_group.venv_path} --copies"
            )

            stdout = execute_command_sync(command=cmd)

            logger.debug(
                (f"END - Create python venv folder {task_group.venv_path}")
            )

            stdout = _customize_and_run_template(
                script_filename="_2_preliminary_pip_operations.sh",
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
            logger.debug("installing - END")

            logger.debug("collecting - START")
            _set_collection_state_data_status(
                state_id=state_id,
                new_status=CollectionStatusV2.COLLECTING,
                logger_name=LOGGER_NAME,
                db=db,
            )

            # Avoid keeping the db session open as we start some possibly
            # long operations that do not use the db
            db.close()

            stdout = _customize_and_run_template(
                script_filename="_5_pip_show.sh",
                **common_args,
            )

            pkg_attrs = parse_script_5_stdout(stdout)
            for key, value in pkg_attrs.items():
                logger.debug(
                    f"collecting - parsed from pip-show: {key}={value}"
                )
            # Check package_name match between pip show and task-group
            package_name_pip_show = pkg_attrs.get("package_name")
            package_name_task_group = task_group.pkg_name
            compare_package_names(
                pkg_name_pip_show=package_name_pip_show,
                pkg_name_task_group=package_name_task_group,
                logger_name=LOGGER_NAME,
            )
            # Extract/drop parsed attributes
            package_name = package_name_task_group
            python_bin = pkg_attrs.pop("python_bin")
            package_root_parent = pkg_attrs.pop("package_root_parent")

            # FIXME SSH: Use more robust logic to determine `package_root`.
            # Examples: use `importlib.util.find_spec`, or parse the output
            # of `pip show --files {package_name}`.
            package_name_underscore = package_name.replace("-", "_")
            package_root = (
                Path(package_root_parent) / package_name_underscore
            ).as_posix()

            # Read and validate manifest file
            manifest_path = pkg_attrs.pop("manifest_path")
            logger.info(f"collecting - now loading {manifest_path=}")
            if not Path(manifest_path).exists():
                raise FileNotFoundError(
                    f"{manifest_path=} not found.\n"
                    "Hint: the manifest file must be at the root "
                    "level of the package directory."
                )
            with open(manifest_path) as json_data:
                pkg_manifest_dict = json.load(json_data)
            logger.info(f"collecting - loaded {manifest_path=}")
            logger.info("collecting - now validating manifest content")
            pkg_manifest = ManifestV2(**pkg_manifest_dict)
            logger.info("collecting - validated manifest content")

            logger.info("collecting - _prepare_tasks_metadata - start")
            task_list = _prepare_tasks_metadata(
                package_manifest=pkg_manifest,
                package_version=task_group.version,
                package_root=Path(package_root),
                python_bin=Path(python_bin),
            )
            logger.info("collecting - _prepare_tasks_metadata - end")

            logger.info(
                "collecting - create_db_tasks_and_update_task_group - " "start"
            )
            task_group = create_db_tasks_and_update_task_group(
                task_list=task_list,
                task_group_id=task_group.id,
                db=db,
            )
            logger.info(
                "collecting - create_db_tasks_and_update_task_group - end"
            )

            logger.debug("collecting - END")

            # Finalize (write metadata to DB)
            logger.debug("finalising - START")

            collection_state = db.get(CollectionStateV2, state_id)
            collection_state.data["log"] = log_file_path.open("r").read()
            collection_state.data["freeze"] = stdout_pip_freeze
            collection_state.data["status"] = CollectionStatusV2.OK
            # FIXME: The `task_list` key is likely not used by any client,
            # we should consider dropping it
            task_read_list = [
                TaskReadV2(**task.model_dump()).dict()
                for task in task_group.task_list
            ]
            collection_state.data["task_list"] = task_read_list
            flag_modified(collection_state, "data")
            db.commit()
            logger.debug("finalising - END")
            logger.debug("END")

        except Exception as e:
            # Delete corrupted package dir
            _handle_failure(
                state_id=state_id,
                log_file_path=log_file_path,
                logger_name=LOGGER_NAME,
                exception=e,
                db=db,
                task_group_id=task_group.id,
            )
            try:
                logger.info(f"Now delete folder {task_group.path}")
                import shutil

                shutil.rmtree(task_group.path)
                logger.info(f"Deleted folder {task_group.path}")
            except Exception as e:
                logger.error(
                    f"Removing folder failed.\n" f"Original error:\n{str(e)}"
                )
            else:
                logger.info(
                    "Not trying to remove folder " f"{task_group.path}."
                )
        return
