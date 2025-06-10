import shlex
import shutil
import subprocess  # nosec
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import add_commit_refresh
from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ..utils_pixi import SOURCE_DIR_NAME
from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityStatusV2
from fractal_server.config import get_settings
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.utils import get_timestamp


def reactivate_local_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
) -> None:
    """
    Reactivate a task group venv.

    This function is run as a background task, therefore exceptions must be
    handled.

    Arguments:
        task_group_id:
        task_group_activity_id:
    """

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )
        with next(get_sync_db()) as db:
            success, task_group, activity = get_activity_and_task_group(
                task_group_activity_id=task_group_activity_id,
                task_group_id=task_group_id,
                db=db,
            )
            if not success:
                return

            # Log some info
            logger.debug("START")

            for key, value in task_group.model_dump().items():
                logger.debug(f"task_group.{key}: {value}")

            source_dir = Path(task_group.path, SOURCE_DIR_NAME).as_posix()
            if Path(source_dir).exists():
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

            try:
                activity.status = TaskGroupActivityStatusV2.ONGOING
                activity = add_commit_refresh(obj=activity, db=db)

                logger.debug("start - writing pixi lock")
                with open(f"{task_group.path}/pixi.lock", "w") as f:
                    f.write(task_group.env_info)
                logger.debug("end - writing pixi lock")

                subprocess.run(  # nosec
                    shlex.split(
                        f"tar xz -f {task_group.archive_path} "
                        f"{Path(task_group.archive_path).name}"
                    ),
                    encoding="utf-8",
                    cwd=task_group.path,
                )

                subprocess.run(  # nosec
                    shlex.split(
                        f"mv {Path(task_group.archive_path).name} {source_dir}"
                    ),
                    encoding="utf-8",
                    cwd=task_group.path,
                )

                settings = Inject(get_settings)
                pixi_home = settings.pixi.versions[task_group.pixi_version]
                pixi_bin = Path(pixi_home, "bin/pixi").as_posix()

                logger.debug("start - pixi install")
                subprocess.run(  # nosec
                    shlex.split(
                        f"{pixi_bin} install "
                        f"--manifest-path {source_dir}/pyproject.toml --frozen"
                    ),
                    encoding="utf-8",
                    cwd=task_group.path,
                )
                logger.debug("end - pixi install")

                activity.log = get_current_log(log_file_path)
                activity.status = TaskGroupActivityStatusV2.OK
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)
                task_group.active = True
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.debug("END")

                reset_logger_handlers(logger)

            except Exception as reactivate_e:
                # Delete corrupted source_dir
                try:
                    logger.info(f"Now delete folder {source_dir}")
                    shutil.rmtree(source_dir)
                    logger.info(f"Deleted folder {source_dir}")
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
        return
