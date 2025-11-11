from devtools import debug

from fractal_server.app.models.v2 import ProjectV2


async def test_project_version_attribute(db):
    project = ProjectV2(name="project")
    db.add(project)
    await db.commit()
    debug(project)
