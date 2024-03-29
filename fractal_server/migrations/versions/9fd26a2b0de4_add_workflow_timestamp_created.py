"""add Workflow.timestamp_created and Dataset.timestamp_created

Revision ID: 9fd26a2b0de4
Revises: efa89c30e0a4
Create Date: 2024-01-11 09:31:20.950090

"""
from datetime import datetime
from datetime import timezone

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "9fd26a2b0de4"
down_revision = "4cedeb448a53"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("workflow", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "timestamp_created",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=str(datetime(2000, 1, 1, tzinfo=timezone.utc)),
            )
        )

    with op.batch_alter_table("workflow", schema=None) as batch_op:
        batch_op.alter_column("timestamp_created", server_default=None)

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "timestamp_created",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=str(datetime(2000, 1, 1, tzinfo=timezone.utc)),
            )
        )

    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.alter_column("timestamp_created", server_default=None)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("dataset", schema=None) as batch_op:
        batch_op.drop_column("timestamp_created")

    with op.batch_alter_table("workflow", schema=None) as batch_op:
        batch_op.drop_column("timestamp_created")

    # ### end Alembic commands ###
