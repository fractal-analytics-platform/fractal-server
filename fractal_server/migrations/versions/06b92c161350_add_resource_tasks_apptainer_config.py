"""add resource.tasks_apptainer_config

Revision ID: 06b92c161350
Revises: 0c5e3f92c3a8
Create Date: 2026-11-05 16:21:56.630343

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "06b92c161350"
down_revision = "0c5e3f92c3a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("resource", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "tasks_apptainer_config",
                postgresql.JSONB(astext_type=sa.Text()),
                server_default="{}",
                nullable=False,
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("resource", schema=None) as batch_op:
        batch_op.drop_column("tasks_apptainer_config")
