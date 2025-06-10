import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ..utils_pixi import SOURCE_DIR_NAME
from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.config import get_settings
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.ssh._utils import _customize_and_run_template
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


def reactivate_ssh_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
) -> None:
    """
    Reactivate a task group venv.

    This function is run as a background task, therefore exceptions must be
    handled.

    Arguments:
        task_group_id:
        task_group_activity_id:
        ssh_config:
        tasks_base_dir:
            Only used as a `safe_root` in `remove_dir`, and typically set to
            `user_settings.ssh_tasks_dir`.
    """

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        with SingleUseFractalSSH(
            ssh_config=ssh_config,
            logger_name=LOGGER_NAME,
        ) as fractal_ssh:

            with next(get_sync_db()) as db:
                success, task_group, activity = get_activity_and_task_group(
                    task_group_activity_id=task_group_activity_id,
                    task_group_id=task_group_id,
                    db=db,
                )
                if not success:
                    return

                # Log some info
                logger.info("START")
                for key, value in task_group.model_dump(
                    exclude={"env_info"}
                ).items():
                    logger.debug(f"task_group.{key}: {value}")

                # Check that SSH connection works
                try:
                    fractal_ssh.check_connection()
                except Exception as e:
                    logger.error("Cannot establish SSH connection.")
                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=e,
                        db=db,
                    )
                    return

                try:

                    settings = Inject(get_settings)
                    replacements = {
                        (
                            "__PIXI_HOME__",
                            settings.pixi.versions[task_group.pixi_version],
                        ),
                        ("__PACKAGE_DIR__", task_group.path),
                        ("__TAR_GZ_PATH__", task_group.archive_path),
                        (
                            "__IMPORT_PACKAGE_NAME__",
                            task_group.pkg_name.replace("-", "_"),
                        ),
                        ("__SOURCE_DIR_NAME__", SOURCE_DIR_NAME),
                        ("__FROZEN_OPTION__", "true"),
                    }

                    logger.info("installing - START")

                    # Set status to ONGOING and refresh logs
                    activity.status = TaskGroupActivityStatusV2.ONGOING
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    script_dir_remote = Path(
                        task_group.path, SCRIPTS_SUBFOLDER
                    ).as_posix()
                    common_args = dict(
                        script_dir_local=(
                            Path(tmpdir) / SCRIPTS_SUBFOLDER
                        ).as_posix(),
                        script_dir_remote=script_dir_remote,
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityActionV2.REACTIVATE}"
                        ),
                        logger_name=LOGGER_NAME,
                        fractal_ssh=fractal_ssh,
                    )

                    # Run the three pixi-related scripts
                    _customize_and_run_template(
                        template_filename="pixi_1_extract.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    pixi_lock_local = Path(tmpdir, "pixi.lock").as_posix()
                    pixi_lock_remote = Path(
                        task_group.path, SOURCE_DIR_NAME, "pixi.lock"
                    ).as_posix()
                    logger.info(
                        f"Write `env_info` contents into {pixi_lock_local}"
                    )
                    with open(pixi_lock_local, "w") as f:
                        f.write(task_group.env_info)
                    fractal_ssh.send_file(
                        local=pixi_lock_local,
                        remote=pixi_lock_remote,
                    )

                    _customize_and_run_template(
                        template_filename="pixi_2_install.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    _customize_and_run_template(
                        template_filename="pixi_3_post_install.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Finalize (write metadata to DB)
                    logger.info("finalising - START")
                    activity.status = TaskGroupActivityStatusV2.OK
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)
                    logger.info("finalising - END")
                    logger.info("END")
                    reset_logger_handlers(logger)

                except Exception as reactivate_e:
                    # Delete corrupted venv_path
                    try:
                        logger.info(
                            f"Now delete folder {task_group.venv_path}"
                        )
                        fractal_ssh.remove_folder(
                            folder=task_group.venv_path,
                            safe_root=tasks_base_dir,
                        )
                        logger.info(f"Deleted folder {task_group.venv_path}")
                    except Exception as rm_e:
                        logger.error(
                            "Removing folder failed.\n"
                            f"Original error:\n{str(rm_e)}"
                        )

                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=reactivate_e,
                        db=db,
                    )
