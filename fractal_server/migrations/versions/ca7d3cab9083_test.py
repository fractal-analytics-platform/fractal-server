"""test

Revision ID: ca7d3cab9083
Revises: 9fd26a2b0de4
Create Date: 2024-01-19 12:13:26.555937

"""
import sqlalchemy as sa
from alembic import op

# https://stackoverflow.com/questions/75350395/how-should-we-manage-datetime-fields-in-sqlmodel-in-python
# https://stackoverflow.com/questions/13370317/sqlalchemy-default-datetime

# revision identifiers, used by Alembic.
revision = "ca7d3cab9083"
down_revision = "9fd26a2b0de4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.alter_column(
            "timestamp_created", server_default=sa.func.now()  # .utcnow() ???
        )
    with op.batch_alter_table("state", schema=None) as batch_op:
        batch_op.alter_column("timestamp", server_default=sa.func.now())
    with op.batch_alter_table("workflow", schema=None) as batch_op:
        batch_op.alter_column(
            "timestamp_created", server_default=sa.func.now()
        )
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.alter_column(
            "timestamp_created", server_default=sa.func.now()
        )
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.alter_column("start_timestamp", server_default=sa.func.now())


def downgrade() -> None:
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.alter_column("timestamp_created", None)
    with op.batch_alter_table("state", schema=None) as batch_op:
        batch_op.alter_column("timestamp", server_default=None)
    with op.batch_alter_table("workflow", schema=None) as batch_op:
        batch_op.alter_column("timestamp_created", server_default=None)
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.alter_column("timestamp_created", server_default=None)
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.alter_column("start_timestamp", server_default=None)
