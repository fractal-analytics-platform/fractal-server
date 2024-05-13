import sys
from copy import deepcopy

from passlib.context import CryptContext
from sqlalchemy import select

from fractal_server.app.db import DB
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v1 import Dataset
from fractal_server.app.models.v1 import Project
from fractal_server.app.models.v1 import State
from fractal_server.app.models.v1 import Task
from fractal_server.app.models.v1 import WorkflowTask


sys.exit("WARNING: executing this script has non-reversible effects!")

sys.exit("WARNING: only execute this script if you are really sure about it")

context = CryptContext(schemes=["bcrypt"], deprecated="auto")
hashed_password = context.hash("1234")


DB.set_sync_db()

with next(get_sync_db()) as db:
    stm = select(Project)
    res = db.execute(stm)
    projects = res.scalars().all()
    for project in projects:
        user_emails = [u.email for u in project.user_list]
        user_ids = [u.id for u in project.user_list]
        if 6 in user_ids or 1 in user_ids:
            pass
        else:
            print("DELETE", user_emails)
            db.delete(project)
            db.commit()

    user = db.get(UserOAuth, 1)
    user.email = "admin@example.org"
    user.cache_dir = "/__REDACTED_CACHE_DIR__"
    user.hashed_password = hashed_password
    db.merge(user)
    db.commit()

    user = db.get(UserOAuth, 6)
    user.email = "user@example.org"
    user.username = "__REDACTED_OWNER__"
    user.slurm_user = "__REDACTED_SLURM_USER_"
    user.cache_dir = "/__REDACTED_CACHE_DIR__"
    user.hashed_password = hashed_password
    db.merge(user)
    db.commit()

    print("-" * 80)

    stm = select(UserOAuth)
    res = db.execute(stm)
    users = res.scalars().unique()
    for user in users:
        if user.id in [1, 6]:
            pass
        else:
            print("DELETE", user.email)
            db.delete(user)
            db.commit()
        print()

    stm = select(Task)
    res = db.execute(stm)
    tasks = res.scalars().all()
    print(f"{len(tasks)=}")
    for task in sorted(tasks, key=lambda obj: obj.id):
        task.command = "/__REDACTED_COMMAND__"
        if task.owner is not None:
            task.owner = "__REDACTED_OWNER__"
            task.source = f"__REDACTED_SOURCE_{task.id}__"
        db.merge(task)
        db.commit()

    stm = select(WorkflowTask)
    res = db.execute(stm)
    workflowtasks = res.scalars().all()
    print(f"{len(workflowtasks)=}")
    for workflowtask in sorted(workflowtasks, key=lambda obj: obj.id):
        if (
            workflowtask.task.name == "Illumination correction"
            or "ultiplexing" in workflowtask.task.name
            or "apari" in workflowtask.task.name
        ):
            workflowtask.args = {}
            db.merge(workflowtask)
            db.commit()
        # sys.exit()

    stm = select(Dataset)
    res = db.execute(stm)
    datasets = res.scalars().all()
    print(f"{len(datasets)=}")
    for dataset in sorted(datasets, key=lambda obj: obj.id):
        for res in dataset.resource_list:
            res.path = "/__REDACTED_RESOURCE_PATH__"
        dataset.history = []
        tmp_meta = deepcopy(dataset.meta)
        tmp_meta["history"] = []
        tmp_meta["original_paths"] = []
        tmp_meta["copy_ome_zarr"] = {}
        dataset.meta = tmp_meta
        dataset = db.merge(dataset)
        db.commit()

    stm = select(ApplyWorkflow)
    res = db.execute(stm)
    jobs = res.scalars().all()
    print(f"{len(jobs)=}")
    for job in sorted(jobs, key=lambda obj: obj.id):
        if job.workflow_dump is None:
            print("workflow_dump is None")

        job.log = "__REDACTED LOGS__"
        job.working_dir = "/__REDACTED_WORKING_DIR__"
        job.working_dir_user = "/__REDACTED_WORKING_DIR_USER__"
        job.workflow_dump = {}
        db.merge(job)
        db.commit()

    stm = select(State)
    res = db.execute(stm)
    states = res.scalars().all()
    for state in states:
        tmp_data = deepcopy(state.data)
        tmp_data["info"] = "__REDACTED_INFO__"
        tmp_data["log"] = "__REDACTED_LOG__"
        tmp_data["venv_path"] = "/__REDACTED_VENV_PATH__"
        tmp_data["task_list"] = []
        state.data = tmp_data
        db.merge(state)
