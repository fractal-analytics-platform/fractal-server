"""Make versions required

Revision ID: c3dd7196d142
Revises: 92ec661b7f76
Create Date: 2026-06-08 14:08:02.552107

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3dd7196d142"
down_revision = "92ec661b7f76"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("taskgroupv2") as batch_op:
        batch_op.alter_column(
            "version", existing_type=sa.VARCHAR(), nullable=False
        )

    with op.batch_alter_table("taskv2") as batch_op:
        batch_op.alter_column(
            "version", existing_type=sa.VARCHAR(), nullable=False
        )


def downgrade() -> None:
    with op.batch_alter_table("taskv2") as batch_op:
        batch_op.alter_column(
            "version", existing_type=sa.VARCHAR(), nullable=True
        )

    with op.batch_alter_table("taskgroupv2") as batch_op:
        batch_op.alter_column(
            "version", existing_type=sa.VARCHAR(), nullable=True
        )
