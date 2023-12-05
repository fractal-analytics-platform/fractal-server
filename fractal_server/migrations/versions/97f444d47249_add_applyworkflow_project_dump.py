"""add ApplyWorkflow.project_dump

Revision ID: 97f444d47249
Revises: d4fe3708d309
Create Date: 2023-12-05 15:36:48.573358

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "97f444d47249"
down_revision = "d4fe3708d309"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "project_dump", sa.JSON(), server_default="{}", nullable=False
            )
        )

    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.alter_column("project_dump", server_default=None)

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.drop_column("project_dump")

    # ### end Alembic commands ###
