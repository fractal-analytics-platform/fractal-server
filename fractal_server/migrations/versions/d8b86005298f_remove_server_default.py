"""Remove server_default

Revision ID: d8b86005298f
Revises: d2c982acb174
Create Date: 2026-06-08 08:55:41.759143

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d8b86005298f"
down_revision = "d2c982acb174"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "version",
            server_default=None,
        )

    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            "version",
            server_default=None,
        )


def downgrade() -> None:
    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            "version",
            server_default="0",
        )

    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "version",
            server_default="0",
        )
