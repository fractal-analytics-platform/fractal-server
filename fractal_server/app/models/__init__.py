from .security import UserOAuth  # noqa: F401

# We include the UserOAuth models to avoid issues with LinkUserProject
# (sometimes taking place in alembic autogenerate)
