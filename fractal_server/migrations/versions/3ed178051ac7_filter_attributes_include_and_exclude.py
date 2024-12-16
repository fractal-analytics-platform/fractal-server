"""filter attributes include and exclude

Revision ID: 3ed178051ac7
Revises: 316140ff7ee1
Create Date: 2024-12-16 16:45:46.245739

"""
import json

from alembic import op


# revision identifiers, used by Alembic.
revision = "3ed178051ac7"
down_revision = "316140ff7ee1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("datasetv2", schema=None) as batch_op:
        batch_op.alter_column(
            "filters",
            server_default=json.dumps(
                attributes_include={}, attributes_exclude={}, types={}
            ),
        )
    with op.batch_alter_table("workflowtaskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "input_filters",
            server_default=json.dumps(
                attributes_include={}, attributes_exclude={}, types={}
            ),
        )


def downgrade() -> None:
    with op.batch_alter_table("workflowtaskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "input_filters", server_default=json.dumps(attributes={}, types={})
        )
    with op.batch_alter_table("datasetv2", schema=None) as batch_op:
        batch_op.alter_column(
            "filters", server_default=json.dumps(attributes={}, types={})
        )
