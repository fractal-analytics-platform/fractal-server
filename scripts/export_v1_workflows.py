import json
import sys
from pathlib import Path

from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v1 import LinkUserProject
from fractal_server.app.models.v1 import Project
from fractal_server.app.models.v1 import Workflow
from fractal_server.logger import set_logger
from fractal_server.string_tools import sanitize_string
from fractal_server.utils import get_timestamp


logger = set_logger(sys.argv[0])

if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError(f"Usage: 'python {sys.argv[0]} folder'")

    folder = Path(sys.argv[1])
    if not folder.exists():
        logger.error(f"Folder {folder} does not exist. Exiting.")
        exit(2)

    timestamp = get_timestamp().strftime("%Y%m%d_%H%M%S")
    base_folder = folder / f"{timestamp}_fractal_v1_workflows"
    confirm = input(
        f"Fractal V1 Workflow will be saved at '{base_folder.resolve()}'. "
        "Do you confirm? [yY]: "
    )

    if confirm not in ["y", "Y"]:
        logger.error(f"Folder {base_folder} not confirmed. Exiting.")
        exit(1)

    base_folder.mkdir(exist_ok=False, parents=False)

    db = next(get_sync_db())

    workflow_list = db.execute(select(Workflow)).scalars().all()
    logger.info(f"Found {len(workflow_list)} V1 workflows to export.")

    for workflow in workflow_list:
        dump = dict(
            **workflow.model_dump(),
            task_list=[
                dict(
                    **workflowtask.model_dump(),
                    task=workflowtask.task.model_dump(),
                )
                for workflowtask in workflow.task_list
            ],
        )
        project_name = db.get(Project, dump["project_id"]).name
        user_email = (
            db.execute(
                select(UserOAuth.email)
                .join(LinkUserProject)
                .where(LinkUserProject.project_id == dump["project_id"])
            )
            .scalars()
            .one_or_none()
        )

        json_path = (
            base_folder
            / sanitize_string(user_email)
            / f"{dump['project_id']}_{sanitize_string(project_name)}"
            / f"{dump['id']}_{sanitize_string(dump['name'])}.json"
        )
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with json_path.open("w") as f:
            json.dump(dump, f, indent=2, sort_keys=True, default=str)

        logger.info(f"Workflow {dump['id']} dumped at '{json_path.resolve()}'")
