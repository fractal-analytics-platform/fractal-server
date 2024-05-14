from .v1 import Project  # noqa: F401
from .v2 import ProjectV2  # noqa: F401

# We include the project models to avoid issues with LinkUserProject
# (sometimes taking place in alembic autogenerate)
