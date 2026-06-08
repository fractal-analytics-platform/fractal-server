"""Make version required

Revision ID: 4d28bb398a9b
Revises: d2c982acb174
Create Date: 2026-06-08 13:12:41.928868

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "4d28bb398a9b"
down_revision = "d2c982acb174"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("taskgroupv2") as batch_op:
        batch_op.alter_column("version", nullable=False)
    with op.batch_alter_table("taskv2") as batch_op:
        batch_op.alter_column("version", nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("taskgroupv2") as batch_op:
        batch_op.alter_column("version", nullable=True)
    with op.batch_alter_table("taskv2") as batch_op:
        batch_op.alter_column("version", nullable=True)
