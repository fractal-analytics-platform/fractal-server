"""Add server default for version

Revision ID: d2c982acb174
Revises: 92ec661b7f76
Create Date: 2026-06-08 08:51:51.443547

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d2c982acb174"
down_revision = "92ec661b7f76"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("taskgroupv2") as batch_op:
        batch_op.alter_column("version", default="0")
    with op.batch_alter_table("taskv2") as batch_op:
        batch_op.alter_column("version", default="0")


def downgrade() -> None:
    with op.batch_alter_table("taskgroupv2") as batch_op:
        batch_op.alter_column("version", default=None)
    with op.batch_alter_table("taskv2") as batch_op:
        batch_op.alter_column("version", default=None)
