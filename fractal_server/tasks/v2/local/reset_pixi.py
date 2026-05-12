import os
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import get_activity_and_task_group
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_pixi import SOURCE_DIR_NAME
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import execute_command_sync
from fractal_server.utils import get_timestamp

from ._utils import _customize_and_run_template
from ._utils import edit_pyproject_toml_in_place_local
from ._utils import rmtree_nofail


def reset_local_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    resource: Resource,
    profile: Profile,
    use_pixi_lockfile: bool,
) -> None:
    """
    Re-collect a task package via pixi.

    This function runs as a background task, therefore exceptions must be
    handled.

    NOTE:  since this function is sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Args:
        task_group_activity_id:
        task_group_id:
        resource:
        profile:
        use_pixi_lockfile:
    """

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        logger.info("START")
        with next(get_sync_db()) as db:
            db_objects_ok, task_group, activity = get_activity_and_task_group(
                task_group_activity_id=task_group_activity_id,
                task_group_id=task_group_id,
                db=db,
                logger_name=LOGGER_NAME,
            )
            if not db_objects_ok:
                return

            try:
                logger.info(f"{use_pixi_lockfile=}")
                frozen_option = "--frozen" if use_pixi_lockfile else ""
                pixi_home = resource.tasks_pixi_config["versions"][
                    task_group.pixi_version
                ]
                pixi_cache_dir = profile.pixi_cache_dir or os.path.join(
                    pixi_home, "cache"
                )
                logger.info(f"Setting PIXI_CACHE_DIR to {pixi_cache_dir}")
                common_args = dict(
                    replacements={
                        ("__PIXI_HOME__", pixi_home),
                        ("__PIXI_CACHE_DIR__", pixi_cache_dir),
                        ("__PACKAGE_DIR__", task_group.path),
                        ("__TAR_GZ_PATH__", task_group.archive_path),
                        (
                            "__IMPORT_PACKAGE_NAME__",
                            task_group.pkg_name.replace("-", "_"),
                        ),
                        ("__SOURCE_DIR_NAME__", SOURCE_DIR_NAME),
                        ("__FROZEN_OPTION__", frozen_option),
                        (
                            "__TOKIO_WORKER_THREADS__",
                            str(
                                resource.tasks_pixi_config[
                                    "TOKIO_WORKER_THREADS"
                                ]
                            ),
                        ),
                        (
                            "__PIXI_CONCURRENT_SOLVES__",
                            str(
                                resource.tasks_pixi_config[
                                    "PIXI_CONCURRENT_SOLVES"
                                ]
                            ),
                        ),
                        (
                            "__PIXI_CONCURRENT_DOWNLOADS__",
                            str(
                                resource.tasks_pixi_config[
                                    "PIXI_CONCURRENT_DOWNLOADS"
                                ]
                            ),
                        ),
                    },
                    script_dir=Path(
                        task_group.path, SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_{TaskGroupActivityAction.RESET}"
                    ),
                    logger_name=LOGGER_NAME,
                )

                activity.status = TaskGroupActivityStatus.ONGOING
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 1
                _customize_and_run_template(
                    template_filename="pixi_1_extract.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Simplify `pyproject.toml`
                source_dir = Path(task_group.path, SOURCE_DIR_NAME).as_posix()
                pyproject_toml_path = Path(source_dir, "pyproject.toml")
                edit_pyproject_toml_in_place_local(
                    pyproject_toml_path, resource=resource
                )

                # Run script 2
                _customize_and_run_template(
                    template_filename="pixi_2_install.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 3
                _customize_and_run_template(
                    template_filename="pixi_3_post_install.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Make task folder 755
                command = f"chmod -R 755 {source_dir}"
                execute_command_sync(
                    command=command,
                    logger_name=LOGGER_NAME,
                )

                # Update task_group data
                logger.info("Add env_info to TaskGroupV2 - start")
                with Path(source_dir, "pixi.lock").open() as f:
                    pixi_lock_contents = f.read()
                task_group.env_info = pixi_lock_contents
                task_group.active = True
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.info("Add env_info to TaskGroupV2 - end")

                # Finalize (write metadata to DB)
                logger.info("finalising - START")
                activity.status = TaskGroupActivityStatus.OK
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)
                logger.info("finalising - END")
                logger.info("END")

                reset_logger_handlers(logger)

            except Exception as e:
                rmtree_nofail(
                    folder_path=source_dir,
                    logger_name=LOGGER_NAME,
                )
                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=e,
                    db=db,
                )
        return
