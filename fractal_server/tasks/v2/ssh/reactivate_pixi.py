import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ..utils_pixi import SOURCE_DIR_NAME
from ._utils import check_ssh_or_fail_and_cleanup
from ._utils import edit_pyproject_toml_in_place_ssh
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

            with SingleUseFractalSSH(
                ssh_config=ssh_config,
                logger_name=LOGGER_NAME,
            ) as fractal_ssh:
                try:
                    # Check SSH connection
                    ssh_ok = check_ssh_or_fail_and_cleanup(
                        fractal_ssh=fractal_ssh,
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        db=db,
                    )
                    if not ssh_ok:
                        return

                    # Check that the (remote) task_group source_dir does not
                    # exist
                    source_dir = Path(
                        task_group.path, SOURCE_DIR_NAME
                    ).as_posix()
                    if fractal_ssh.remote_exists(source_dir):
                        error_msg = f"{source_dir} already exists."
                        logger.error(error_msg)
                        fail_and_cleanup(
                            task_group=task_group,
                            task_group_activity=activity,
                            logger_name=LOGGER_NAME,
                            log_file_path=log_file_path,
                            exception=FileExistsError(error_msg),
                            db=db,
                        )
                        return

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
                        ("__FROZEN_OPTION__", "--frozen"),
                        (
                            "__TOKIO_WORKER_THREADS__",
                            str(settings.pixi.TOKIO_WORKER_THREADS),
                        ),
                        (
                            "__PIXI_CONCURRENT_SOLVES__",
                            str(settings.pixi.PIXI_CONCURRENT_SOLVES),
                        ),
                        (
                            "__PIXI_CONCURRENT_DOWNLOADS__",
                            str(settings.pixi.PIXI_CONCURRENT_DOWNLOADS),
                        ),
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

                    # Run script 1 - extract tar.gz into `source_dir`
                    stdout = _customize_and_run_template(
                        template_filename="pixi_1_extract.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    logger.debug(f"STDOUT: {stdout}")
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Simplify `pyproject.toml`
                    source_dir = Path(
                        task_group.path, SOURCE_DIR_NAME
                    ).as_posix()
                    pyproject_toml_path = Path(source_dir, "pyproject.toml")
                    edit_pyproject_toml_in_place_ssh(
                        fractal_ssh=fractal_ssh,
                        pyproject_toml_path=pyproject_toml_path,
                    )
                    # Write pixi.lock into `source_dir`
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

                    # Run script 2 - run pixi-install command
                    stdout = _customize_and_run_template(
                        template_filename="pixi_2_install.sh",
                        replacements=replacements,
                        login_shell=True,
                        **common_args,
                    )
                    logger.debug(f"STDOUT: {stdout}")
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Run script 3 - post-install
                    stdout = _customize_and_run_template(
                        template_filename="pixi_3_post_install.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    logger.debug(f"STDOUT: {stdout}")
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    fractal_ssh.run_command(cmd=f"chmod -R 755 {source_dir}")

                    # Finalize (write metadata to DB)
                    activity.status = TaskGroupActivityStatusV2.OK
                    activity.timestamp_ended = get_timestamp()
                    activity = add_commit_refresh(obj=activity, db=db)
                    task_group.active = True
                    task_group = add_commit_refresh(obj=task_group, db=db)
                    logger.info("END")

                    reset_logger_handlers(logger)

                except Exception as reactivate_e:
                    # Delete corrupted source_dir
                    try:
                        logger.info(f"Now delete folder {source_dir}")
                        fractal_ssh.remove_folder(
                            folder=source_dir,
                            safe_root=tasks_base_dir,
                        )
                        logger.info(f"Deleted folder {source_dir}")
                    except Exception as rm_e:
                        logger.error(
                            "Removing folder failed. "
                            f"Original error: {str(rm_e)}"
                        )

                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        logger_name=LOGGER_NAME,
                        log_file_path=log_file_path,
                        exception=reactivate_e,
                        db=db,
                    )
