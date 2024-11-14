from devtools import debug

from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import ProjectReadV2


async def test_project_timestamp_timezone(db):
    """
    Related to
    https://www.psycopg.org/psycopg3/docs/basic/adapt.html#date-time-types-adaptation
    """

    project = ProjectV2(name="project")
    db.add(project)
    await db.commit()
    await db.refresh(project)

    debug((await db.connection()))

    debug(project.timestamp_created)
    assert project.timestamp_created.tzinfo is not None

    project_read = ProjectReadV2(**project.model_dump())
    debug(project_read.timestamp_created)
    assert project_read.timestamp_created.tzinfo is not None
