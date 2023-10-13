"""Add Dataset.history

Revision ID: 5c4ff7526508
Revises: 8f79bd162e35
Create Date: 2023-10-13 14:26:07.955329

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql import column
from sqlalchemy.sql import table


# revision identifiers, used by Alembic.
revision = "5c4ff7526508"
down_revision = "8f79bd162e35"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.add_column(sa.Column("history", sa.JSON(), nullable=True))

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        dataset = table("dataset", column("history"))
        batch_op.execute(dataset.update().values(history=[]))

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.alter_column("dataset", nullable=False)


def downgrade() -> None:
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_column("history")
