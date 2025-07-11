import json
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_database import create_db_tasks_and_update_task_group_sync
from ..utils_pixi import parse_collect_stdout
from ..utils_pixi import SOURCE_DIR_NAME
from ._utils import edit_pyproject_toml_in_place_local
from fractal_server.app.db import get_sync_db
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
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import (
    get_activity_and_task_group,
)
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_background import prepare_tasks_metadata
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import execute_command_sync
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

            if Path(task_group.path).exists():
                # We handle this before the try/except to avoid the rmtree
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

                common_args = dict(
                    replacements={
                        (
                            "__PIXI_HOME__",
                            settings.pixi.versions[task_group.pixi_version],
                        ),
                        ("__PACKAGE_DIR__", task_group.path),
                        ("__TAR_GZ_PATH__", archive_path),
                        (
                            "__IMPORT_PACKAGE_NAME__",
                            task_group.pkg_name.replace("-", "_"),
                        ),
                        ("__SOURCE_DIR_NAME__", SOURCE_DIR_NAME),
                        ("__FROZEN_OPTION__", ""),
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
                    },
                    script_dir=Path(
                        task_group.path, SCRIPTS_SUBFOLDER
                    ).as_posix(),
                    prefix=(
                        f"{int(time.time())}_"
                        f"{TaskGroupActivityActionV2.COLLECT}"
                    ),
                    logger_name=LOGGER_NAME,
                )

                activity.status = TaskGroupActivityStatusV2.ONGOING
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
                edit_pyproject_toml_in_place_local(pyproject_toml_path)

                # Run script 2
                _customize_and_run_template(
                    template_filename="pixi_2_install.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Run script 3
                stdout = _customize_and_run_template(
                    template_filename="pixi_3_post_install.sh",
                    **common_args,
                )
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                # Parse stdout
                parsed_output = parse_collect_stdout(stdout)
                package_root = parsed_output["package_root"]
                venv_size = parsed_output["venv_size"]
                venv_file_number = parsed_output["venv_file_number"]
                project_python_wrapper = parsed_output[
                    "project_python_wrapper"
                ]

                # Make task folder 755
                command = f"chmod -R 755 {source_dir}"
                execute_command_sync(
                    command=command,
                    logger_name=LOGGER_NAME,
                )

                # Read and validate manifest
                # NOTE: we are only supporting the manifest path being relative
                # to the top-level folder
                manifest_path = f"{package_root}/__FRACTAL_MANIFEST__.json"
                with open(manifest_path) as json_data:
                    pkg_manifest_dict = json.load(json_data)
                logger.info(f"loaded {manifest_path=}")
                logger.info("now validating manifest content")
                pkg_manifest = ManifestV2(**pkg_manifest_dict)
                logger.info("validated manifest content")
                activity.log = get_current_log(log_file_path)
                activity = add_commit_refresh(obj=activity, db=db)

                logger.info("_prepare_tasks_metadata - start")
                task_list = prepare_tasks_metadata(
                    package_manifest=pkg_manifest,
                    package_version=task_group.version,
                    package_root=Path(package_root),
                    project_python_wrapper=Path(project_python_wrapper),
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
                    "Add env_info, venv_size and venv_file_number "
                    "to TaskGroupV2 - start"
                )
                with Path(source_dir, "pixi.lock").open() as f:
                    pixi_lock_contents = f.read()

                # NOTE: see issue 2626 about whether to keep `pixi.lock` files
                # in the database
                task_group.env_info = pixi_lock_contents
                task_group.venv_size_in_kB = int(venv_size)
                task_group.venv_file_number = int(venv_file_number)
                task_group = add_commit_refresh(obj=task_group, db=db)
                logger.info(
                    "Add env_info, venv_size and venv_file_number "
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
                try:
                    logger.info(f"Now delete folder {task_group.path}")
                    shutil.rmtree(task_group.path)
                    logger.info(f"Deleted folder {task_group.path}")
                except Exception as rm_e:
                    logger.error(
                        f"Removing folder failed. Original error: {str(rm_e)}"
                    )
                fail_and_cleanup(
                    task_group=task_group,
                    task_group_activity=activity,
                    logger_name=LOGGER_NAME,
                    log_file_path=log_file_path,
                    exception=collection_e,
                    db=db,
                )
