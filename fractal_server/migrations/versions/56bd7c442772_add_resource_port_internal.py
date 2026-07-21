"""Add resource.port_internal

Revision ID: 56bd7c442772
Revises: d4027db95431
Create Date: 2026-07-21 11:29:11.631696

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "56bd7c442772"
down_revision = "d4027db95431"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("resource", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("port_internal", sa.Integer(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("resource", schema=None) as batch_op:
        batch_op.drop_column("port_internal")
