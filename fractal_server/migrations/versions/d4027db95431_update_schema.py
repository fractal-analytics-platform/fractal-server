"""Update  schema

Revision ID: d4027db95431
Revises: daecd41f37da
Create Date: 2026-07-07 21:20:41.395527

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d4027db95431"
down_revision = "daecd41f37da"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table(table_name="historyunit") as batch_op:
        batch_op.alter_column(column_name="has_warnings", server_default=None)


def downgrade() -> None:
    with op.batch_alter_table(table_name="historyunit") as batch_op:
        batch_op.alter_column(
            column_name="has_warnings", server_default="false"
        )
