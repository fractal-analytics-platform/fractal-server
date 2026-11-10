import time
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy.exc import NoResultFound

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.utils import MANIFEST_FILENAME
from fractal_server.tasks.utils import TASK_GROUP_ID_FILENAME
from fractal_server.tasks.v2.ssh._utils import _customize_and_run_template
from fractal_server.tasks.v2.ssh._utils import _customize_and_send_template
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import fail_and_cleanup
from fractal_server.tasks.v2.utils_background import get_activity_and_task_group
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_background import prepare_tasks_metadata
from fractal_server.tasks.v2.utils_database import (
    create_db_tasks_and_update_task_group_sync,
)
from fractal_server.tasks.v2.utils_pixi import SOURCE_DIR_NAME
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp

from ._pixi_slurm_ssh import run_script_on_remote_slurm
from ._utils import check_ssh_or_fail_and_cleanup

CONTAINER_ROOT = "/"
DEF_FILENAME = "apptainer.def"
SIF_FILENAME = "image.sif"
PIXI_LOCK_FILENAME = "pixi.lock"


def _parse_package_root(stdout: str) -> str:
    """
    Extract the package folder (a path inside the container) from the
    stdout of `apptainer_3_post_build.sh`.
    """
    PACKAGE_FOLDER_PREFIX = "Package folder:"
    matching_lines = [
        line for line in stdout.splitlines() if PACKAGE_FOLDER_PREFIX in line
    ]
    if len(matching_lines) != 1:
        raise ValueError(
            f"String '{PACKAGE_FOLDER_PREFIX}' found {len(matching_lines)} "
            "times in stdout (expected once)."
        )
    return matching_lines[0].split(PACKAGE_FOLDER_PREFIX)[-1].strip(" ")


def collect_ssh_apptainer(
    *,
    task_group_id: int,
    task_group_activity_id: int,
    tar_gz_file: FractalUploadedFile,
    resource: Resource,
    profile: Profile,
) -> None:
    LOGGER_NAME = f"{__name__}.ID{task_group_activity_id}"

    # Work within a temporary folder, where also logs will be placed
    with TemporaryDirectory() as tmpdir:
        log_file_path = Path(tmpdir) / "log"
        logger = set_logger(
            logger_name=LOGGER_NAME,
            log_file_path=log_file_path,
        )
        logger.info("START")
        with next(get_sync_db()) as db:
            try:
                task_group, activity = get_activity_and_task_group(
                    task_group_activity_id=task_group_activity_id,
                    task_group_id=task_group_id,
                    db=db,
                    logger_name=LOGGER_NAME,
                )
            except NoResultFound:
                return

            with SingleUseFractalSSH(
                ssh_config=SSHConfig(
                    host=resource.host,
                    user=profile.username,
                    key_path=profile.ssh_key_path,
                ),
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

                    # Check that the (remote) task_group path does not exist
                    if fractal_ssh.remote_exists(task_group.path):
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

                    # Create remote `task_group.path` and `script_dir_remote`
                    # folders
                    script_dir_remote = Path(
                        task_group.path, SCRIPTS_SUBFOLDER
                    ).as_posix()
                    fractal_ssh.mkdir(folder=task_group.path, parents=True)
                    fractal_ssh.mkdir(folder=script_dir_remote, parents=True)
                    fractal_ssh.write_remote_file(
                        path=f"{task_group.path}/{TASK_GROUP_ID_FILENAME}",
                        content=str(task_group_id),
                    )
                    # Write tar.gz file locally and send it to remote path,
                    # and set task_group.archive_path
                    tar_gz_filename = tar_gz_file.filename
                    archive_path = (
                        Path(task_group.path) / tar_gz_filename
                    ).as_posix()
                    tmp_archive_path = Path(tmpdir, tar_gz_filename).as_posix()
                    logger.info(f"Write tar.gz file into {tmp_archive_path}")
                    with open(tmp_archive_path, "wb") as f:
                        f.write(tar_gz_file.contents)
                    fractal_ssh.send_file(
                        local=tmp_archive_path,
                        remote=archive_path,
                    )
                    task_group.archive_path = archive_path
                    task_group = add_commit_refresh(obj=task_group, db=db)

                    apptainer_config = resource.tasks_apptainer_config
                    def_file_path = Path(
                        task_group.path, DEF_FILENAME
                    ).as_posix()
                    sif_path = Path(task_group.path, SIF_FILENAME).as_posix()
                    container_pyproject_toml = Path(
                        CONTAINER_ROOT, SOURCE_DIR_NAME, "pyproject.toml"
                    ).as_posix()
                    # Command that runs Python inside the container
                    python_command = (
                        f"{apptainer_config['apptainer_bin']} exec "
                        "--env PYTHONNOUSERSITE=1 "
                        f"{sif_path} "
                        "pixi run --frozen "
                        f"--manifest-path {container_pyproject_toml} "
                        "python"
                    )
                    manifest_path_remote = Path(
                        task_group.path, MANIFEST_FILENAME
                    ).as_posix()
                    pixi_lock_path_remote = Path(
                        task_group.path, PIXI_LOCK_FILENAME
                    ).as_posix()
                    replacements = {
                        (
                            "__APPTAINER_BIN__",
                            apptainer_config["apptainer_bin"],
                        ),
                        ("__PIXI_VERSION__", task_group.pixi_version),
                        ("__PACKAGE_DIR__", task_group.path),
                        ("__TAR_GZ_PATH__", task_group.archive_path),
                        ("__SOURCE_DIR_NAME__", SOURCE_DIR_NAME),
                        ("__CONTAINER_ROOT__", CONTAINER_ROOT),
                        ("__DEF_FILE__", def_file_path),
                        ("__SIF_PATH__", sif_path),
                        (
                            "__IMPORT_PACKAGE_NAME__",
                            task_group.pkg_name.replace("-", "_"),
                        ),
                        ("__PYTHON_COMMAND__", python_command),
                        ("__MANIFEST_FILENAME__", MANIFEST_FILENAME),
                        ("__MANIFEST_PATH__", manifest_path_remote),
                        ("__PIXI_LOCK_PATH__", pixi_lock_path_remote),
                    }

                    logger.info("installing - START")

                    # Set status to ONGOING and refresh logs
                    activity.status = TaskGroupActivityStatus.ONGOING
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    common_args = dict(
                        script_dir_local=(
                            Path(tmpdir, SCRIPTS_SUBFOLDER)
                        ).as_posix(),
                        script_dir_remote=script_dir_remote,
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityAction.COLLECT}"
                        ),
                        logger_name=LOGGER_NAME,
                    )

                    # Write the definition file (over SSH)
                    stdout = _customize_and_run_template(
                        template_filename="apptainer_1_write_def.sh",
                        replacements=replacements,
                        fractal_ssh=fractal_ssh,
                        **common_args,
                    )
                    logger.debug(f"STDOUT: {stdout}")
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Prepare scripts 2 and 3
                    remote_script2_path = _customize_and_send_template(
                        template_filename="apptainer_2_build.sh",
                        replacements=replacements,
                        fractal_ssh=fractal_ssh,
                        **common_args,
                    )
                    remote_script3_path = _customize_and_send_template(
                        template_filename="apptainer_3_post_build.sh",
                        replacements=replacements,
                        fractal_ssh=fractal_ssh,
                        **common_args,
                    )
                    logger.debug(
                        f"Build script written to {remote_script2_path=}."
                    )
                    logger.debug(
                        f"Post-build script written to {remote_script3_path=}."
                    )
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Run scripts 2 and 3
                    stdout = run_script_on_remote_slurm(
                        job_name=LOGGER_NAME,
                        script_paths=[
                            remote_script2_path,
                            remote_script3_path,
                        ],
                        final_commands=[],
                        slurm_config=apptainer_config["SLURM_CONFIG"],
                        fractal_ssh=fractal_ssh,
                        logger_name=LOGGER_NAME,
                        prefix=common_args["prefix"],
                        db=db,
                        activity=activity,
                        log_file_path=log_file_path,
                        poll_interval=resource.jobs_poll_interval,
                    )
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Parse stdout. NOTE: `package_root` is a path inside the
                    # container.
                    package_root = _parse_package_root(stdout)

                    # Read and validate manifest file (copied out of the image)
                    pkg_manifest_dict = fractal_ssh.read_remote_json_file(
                        manifest_path_remote
                    )
                    logger.info(f"Loaded {manifest_path_remote=}")
                    pkg_manifest = ManifestV2(**pkg_manifest_dict)
                    logger.info("Manifest is a valid ManifestV2")

                    logger.info("_prepare_tasks_metadata - start")
                    task_list = prepare_tasks_metadata(
                        package_manifest=pkg_manifest,
                        package_version=task_group.version,
                        package_root=Path(package_root),
                        python_bin=Path(python_command),
                    )
                    logger.info("_prepare_tasks_metadata - end")

                    logger.info("create_db_tasks_and_update_task_group - start")
                    create_db_tasks_and_update_task_group_sync(
                        task_list=task_list,
                        task_group_id=task_group.id,
                        db=db,
                    )
                    logger.info("create_db_tasks_and_update_task_group - end")

                    # Update task_group data
                    logger.info("Add env_info to TaskGroupV2 - start")
                    task_group.env_info = fractal_ssh.read_remote_text_file(
                        pixi_lock_path_remote
                    )
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

                except Exception as collection_e:
                    # Delete corrupted package dir
                    try:
                        logger.info(
                            f"Now delete remote folder {task_group.path}"
                        )
                        fractal_ssh.remove_folder(folder=task_group.path)
                        logger.info(f"Deleted remoted folder {task_group.path}")
                    except Exception as e_rm:
                        logger.error(
                            "Removing folder failed. "
                            f"Original error: {str(e_rm)}"
                        )
                    fail_and_cleanup(
                        task_group=task_group,
                        task_group_activity=activity,
                        log_file_path=log_file_path,
                        logger_name=LOGGER_NAME,
                        exception=collection_e,
                        db=db,
                    )
