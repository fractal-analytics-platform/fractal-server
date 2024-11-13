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


if len(sys.argv) != 2:
    raise ValueError(f"Usage: 'python {sys.argv[0]} folder'")
base_folder = Path(sys.argv[1])

logger = set_logger(sys.argv[0])

if __name__ == "__main__":

    db = next(get_sync_db())

    workflow_list = db.execute(select(Workflow)).scalars().all()

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
        logger.info(f"Workflow {dump['id']} dumped at '{json_path}'")
