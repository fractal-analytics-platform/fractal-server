import json
import logging
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_database import create_db_tasks_and_update_task_group_sync
from ..utils_pixi import parse_collect_stdout
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.config import get_settings
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import get_log_path
from fractal_server.tasks.v2.local._utils import _customize_and_run_template
from fractal_server.tasks.v2.local._utils import check_task_files_exist
from fractal_server.tasks.v2.utils_background import _prepare_tasks_metadata
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


def collect_local_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    tar_gz_file: FractalUploadedFile,
) -> None:
    settings = Inject(get_settings)

    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    with TemporaryDirectory() as tmpdir:
        log_file_path = get_log_path(Path(tmpdir))
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )

        with next(get_sync_db()) as db:
            activity = db.get(TaskGroupActivityV2, task_group_activity_id)
            task_group = db.get(TaskGroupV2, task_group_id)
            if activity is None or task_group is None:
                logging.error(
                    "Cannot find database rows with "
                    f"{task_group_id=} and {task_group_activity_id=}:\n"
                    f"{task_group=}\n{activity=}. Exit."
                )
                return

            logger.info("START")
            for key, value in task_group.model_dump().items():
                logger.debug(f"task_group.{key}: {value}")

            if Path(task_group.path).exists():
                error_msg = f"{task_group.path} already exists."
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

            # Set `pixi_bin` and check that it exists
            pixi_bin = settings.pixi.versions[task_group.pixi_version]
            if not Path(pixi_bin).exists():
                error_msg = f"{pixi_bin} does not exist."
                logger.error(error_msg)
                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=FileNotFoundError(error_msg),
                    db=db,
                )
                return

            try:
                Path(task_group.path).mkdir(parents=True)
                logger.info(f"Created {task_group.path}")

                archive_path = Path(
                    task_group.path, tar_gz_file.filename
                ).as_posix()
                logger.info(f"Write tar.gz-file contents into {archive_path}.")
                with open(archive_path, "wb") as f:
                    f.write(tar_gz_file.contents)
                task_group.archive_path = archive_path
                task_group = add_commit_refresh(obj=task_group, db=db)

                replacements = {
                    ("__PIXI_HOME__", pixi_bin),
                    ("__PACKAGE_DIR__", task_group.path),
                    ("__TAR_GZ_PATH__", archive_path),
                    ("__PACKAGE_NAME__", task_group.pkg_name),
                }

                activity.status = TaskGroupActivityStatusV2.ONGOING
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                stdout = _customize_and_run_template(
                    template_filename="pixi/1_collect.sh",
                    replacements=replacements,
                    script_dir=(
                        Path(task_group.path) / SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_"
                        f"{TaskGroupActivityActionV2.COLLECT}_"
                    ),
                    logger_name=LOGGER_NAME,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Parse stdout
                parsed_output = parse_collect_stdout(stdout)
                package_root = parsed_output["package_root"]
                venv_size = parsed_output["venv_size"]
                venv_file_number = parsed_output["venv_file_number"]

                # TODO: check that this is the right path:
                # TODO (later): expose more flexibility (or maybe not)
                manifest_path = f"{package_root}/__FRACTAL_MANIFEST__.json"
                if not Path(manifest_path).exists():
                    raise ValueError(f"Manifest not found at {manifest_path}")
                with open(manifest_path) as json_data:
                    pkg_manifest_dict = json.load(json_data)
                logger.info(f"loaded {manifest_path=}")
                logger.info("now validating manifest content")
                pkg_manifest = ManifestV2(**pkg_manifest_dict)
                logger.info("validated manifest content")
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                logger.info("_prepare_tasks_metadata - start")
                task_list = _prepare_tasks_metadata(
                    package_manifest=pkg_manifest,
                    package_version=task_group.version,
                    package_root=Path(package_root),
                    pixi_bin=pixi_bin,
                )
                check_task_files_exist(task_list=task_list)
                logger.info("_prepare_tasks_metadata - end")
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                logger.info("create_db_tasks_and_update_task_group - start")
                create_db_tasks_and_update_task_group_sync(
                    task_list=task_list,
                    task_group_id=task_group.id,
                    db=db,
                )
                logger.info("create_db_tasks_and_update_task_group - end")

                # Update task_group data
                logger.info(
                    "Add pip_freeze, venv_size and venv_file_number "
                    "to TaskGroupV2 - start"
                )
                # FIXME: rename `pip_freeze` into `env_info`
                with Path(task_group.path, "source_dir/pixi.lock").open() as f:
                    pixi_lock_contents = f.read()
                task_group.pip_freeze = pixi_lock_contents
                task_group.venv_size_in_kB = int(venv_size)
                task_group.venv_file_number = int(venv_file_number)
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.info(
                    "Add pip_freeze, venv_size and venv_file_number "
                    "to TaskGroupV2 - end"
                )

                # Finalize (write metadata to DB)
                logger.info("finalising - START")
                activity.status = TaskGroupActivityStatusV2.OK
                activity.timestamp_ended = get_timestamp()
                activity = add_commit_refresh(obj=activity, db=db)
                logger.info("finalising - END")
                logger.info("END")

                reset_logger_handlers(logger)

            except Exception as collection_e:
                # Delete corrupted package dir
                try:
                    logger.info(f"Now delete folder {task_group.path}")
                    shutil.rmtree(task_group.path)
                    logger.info(f"Deleted folder {task_group.path}")
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
                    exception=collection_e,
                    db=db,
                )
