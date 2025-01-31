"""
Note that this module is imported from `fractal_server/migrations/env.py`,
thus we should always export all relevant database models from here or they
will not be picked up by alembic.
"""
from .linkusergroup import LinkUserGroup  # noqa: F401
from .linkuserproject import LinkUserProjectV2  # noqa: F401
from .security import *  # noqa
from .user_settings import UserSettings  # noqa
from .v2 import *  # noqa
