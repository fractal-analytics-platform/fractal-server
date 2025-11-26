import sys

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth


def fix_db():
    with next(get_sync_db()) as db:
        res = db.execute(select(UserOAuth))
        user_list = res.scalars().unique().all()

        for user in user_list:
            if user.project_dirs != []:
                sys.exit(f"Non empty `project_dirs` for User[{user.id}]")
            user.project_dirs.append(user.project_dir)
            flag_modified(user, "project_dirs")

        db.commit()
