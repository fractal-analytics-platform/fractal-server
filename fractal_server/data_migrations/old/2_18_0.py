import logging
import sys
from os.path import normpath

from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import UserOAuth

logging.basicConfig(level=logging.INFO)


def fix_db():
    logging.info("START - fix db")

    with next(get_sync_db()) as db:
        res = db.execute(select(UserOAuth).order_by(UserOAuth.email))
        user_list = res.scalars().unique().all()

        for user in user_list:
            logging.info(f"Now handling user {user.email}.")
            if user.project_dirs != []:
                sys.exit(f"Non empty `project_dirs` for User[{user.id}]")
            user.project_dirs.append(normpath(user.project_dir))
            flag_modified(user, "project_dirs")

        db.commit()

    logging.info("END - fix db")
