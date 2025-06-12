from sqlmodel import func
from sqlmodel import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import TaskGroupV2
from fractal_server.logger import set_logger


def check_taskgroup_path_unique() -> None:
    with next(get_sync_db()) as db:
        results = db.execute(
            select(TaskGroupV2.path, func.array_agg(TaskGroupV2.id))
            .where(TaskGroupV2.path.isnot(None))
            .group_by(TaskGroupV2.path)
        ).all()
        not_unique_paths = [res for res in results if len(res[1]) > 1]
        if not_unique_paths:
            logger = set_logger(__name__)
            for not_unique_path, ids in not_unique_paths:
                logger.error(
                    f"Path '{not_unique_path}' is repeated in TaskGroups {ids}"
                )
            exit(1)
        return


if __name__ == "__main__":
    check_taskgroup_path_unique()
