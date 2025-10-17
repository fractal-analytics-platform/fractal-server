import time
from pathlib import Path
from tempfile import TemporaryDirectory

from ..utils_background import fail_and_cleanup
from ..utils_background import get_activity_and_task_group
from ..utils_background import prepare_tasks_metadata
from ..utils_database import create_db_tasks_and_update_task_group_sync
from ..utils_pixi import parse_collect_stdout
from ..utils_pixi import SOURCE_DIR_NAME
from ._pixi_slurm_ssh import run_script_on_remote_slurm
from ._utils import check_ssh_or_fail_and_cleanup
from ._utils import edit_pyproject_toml_in_place_ssh
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityActionV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import SingleUseFractalSSH
from fractal_server.ssh._fabric import SSHConfig
from fractal_server.tasks.v2.ssh._utils import _customize_and_run_template
from fractal_server.tasks.v2.ssh._utils import _customize_and_send_template
from fractal_server.tasks.v2.utils_background import add_commit_refresh
from fractal_server.tasks.v2.utils_background import get_current_log
from fractal_server.tasks.v2.utils_templates import SCRIPTS_SUBFOLDER
from fractal_server.utils import get_timestamp


def collect_ssh_pixi(
    *,
    task_group_id: int,
    task_group_activity_id: int,
    tar_gz_file: FractalUploadedFile,
    resource: Resource,
    profile: Profile,
) -> None:
    """
    Collect a task package over SSH

    This function runs as a background task, therefore exceptions must be
    handled.

    NOTE: since this function is sync, it runs within a thread - due to
    starlette/fastapi handling of background tasks (see
    https://github.com/encode/starlette/blob/master/starlette/background.py).


    Args:
        task_group_id:
        task_group_activity_id:
        ssh_config:
        tar_gz_file:
    """

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
            db_objects_ok, task_group, activity = get_activity_and_task_group(
                task_group_activity_id=task_group_activity_id,
                task_group_id=task_group_id,
                db=db,
                logger_name=LOGGER_NAME,
            )
            if not db_objects_ok:
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

                    replacements = {
                        (
                            "__PIXI_HOME__",
                            resource.tasks_pixi_config["versions"][
                                task_group.pixi_version
                            ],
                        ),
                        ("__PACKAGE_DIR__", task_group.path),
                        ("__TAR_GZ_PATH__", task_group.archive_path),
                        (
                            "__IMPORT_PACKAGE_NAME__",
                            task_group.pkg_name.replace("-", "_"),
                        ),
                        ("__SOURCE_DIR_NAME__", SOURCE_DIR_NAME),
                        ("__FROZEN_OPTION__", ""),
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
                    }

                    logger.info("installing - START")

                    # Set status to ONGOING and refresh logs
                    activity.status = TaskGroupActivityStatusV2.ONGOING
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    common_args = dict(
                        script_dir_local=(
                            Path(tmpdir, SCRIPTS_SUBFOLDER)
                        ).as_posix(),
                        script_dir_remote=script_dir_remote,
                        prefix=(
                            f"{int(time.time())}_"
                            f"{TaskGroupActivityActionV2.COLLECT}"
                        ),
                        logger_name=LOGGER_NAME,
                        fractal_ssh=fractal_ssh,
                    )

                    # Run the three pixi-related scripts
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
                        resource=resource,
                    )

                    # Prepare scripts 2 and 3
                    remote_script2_path = _customize_and_send_template(
                        template_filename="pixi_2_install.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    remote_script3_path = _customize_and_send_template(
                        template_filename="pixi_3_post_install.sh",
                        replacements=replacements,
                        **common_args,
                    )
                    logger.debug(
                        "Post-installation script written to "
                        f"{remote_script3_path=}."
                    )
                    logger.debug(
                        "Installation script written to "
                        f"{remote_script2_path=}."
                    )
                    activity.log = get_current_log(log_file_path)
                    activity = add_commit_refresh(obj=activity, db=db)

                    # Run scripts 2 and 3
                    stdout = run_script_on_remote_slurm(
                        script_paths=[
                            remote_script2_path,
                            remote_script3_path,
                            f"chmod -R 755 {source_dir}",
                        ],
                        slurm_config=resource.tasks_pixi_config[
                            "SLURM_CONFIG"
                        ],
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

                    # Parse stdout
                    parsed_output = parse_collect_stdout(stdout)
                    package_root_remote = parsed_output["package_root"]
                    venv_size = parsed_output["venv_size"]
                    venv_file_number = parsed_output["venv_file_number"]
                    project_python_wrapper = parsed_output[
                        "project_python_wrapper"
                    ]

                    # Read and validate remote manifest file
                    manifest_path_remote = (
                        f"{package_root_remote}/__FRACTAL_MANIFEST__.json"
                    )
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
                        package_root=Path(package_root_remote),
                        project_python_wrapper=Path(project_python_wrapper),
                    )
                    logger.info("_prepare_tasks_metadata - end")

                    logger.info(
                        "create_db_tasks_and_update_task_group - " "start"
                    )
                    create_db_tasks_and_update_task_group_sync(
                        task_list=task_list,
                        task_group_id=task_group.id,
                        db=db,
                    )
                    logger.info("create_db_tasks_and_update_task_group - end")

                    # NOTE: see issue 2626 about whether to keep `pixi.lock`
                    # files in the database
                    remote_pixi_lock_file = Path(
                        task_group.path,
                        SOURCE_DIR_NAME,
                        "pixi.lock",
                    ).as_posix()
                    pixi_lock_contents = fractal_ssh.read_remote_text_file(
                        remote_pixi_lock_file
                    )

                    # Update task_group data
                    logger.info(
                        "Add env_info, venv_size and venv_file_number "
                        "to TaskGroupV2 - start"
                    )
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
                    # Delete corrupted package dir
                    try:
                        logger.info(
                            f"Now delete remote folder {task_group.path}"
                        )
                        fractal_ssh.remove_folder(
                            folder=task_group.path,
                            safe_root=profile.tasks_remote_dir,
                        )
                        logger.info(
                            f"Deleted remoted folder {task_group.path}"
                        )
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
