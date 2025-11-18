"""Remove project_dir server_default

Revision ID: 7673fe18c05d
Revises: 49d0856e9569
Create Date: 2025-11-11 16:50:20.079193

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "7673fe18c05d"
down_revision = "49d0856e9569"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Remove `server_default` for `project_dir` column - see
    https://alembic.sqlalchemy.org/en/latest/ops.html#alembic.operations.Operations.alter_column.params.server_default
    """
    with op.batch_alter_table("user_oauth") as batch_op:
        batch_op.alter_column("project_dir", server_default=None)


def downgrade() -> None:
    with op.batch_alter_table("user_oauth") as batch_op:
        batch_op.alter_column("project_dir", server_default="/PLACEHOLDER")
