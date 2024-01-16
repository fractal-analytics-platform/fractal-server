"""WorkflowTask foreign keys not nullables

Revision ID: 4cedeb448a53
Revises: efa89c30e0a4
Create Date: 2024-01-16 13:57:47.891931

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "4cedeb448a53"
down_revision = "efa89c30e0a4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("workflowtask", schema=None) as batch_op:
        batch_op.alter_column(
            "workflow_id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.alter_column(
            "task_id", existing_type=sa.INTEGER(), nullable=False
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("workflowtask", schema=None) as batch_op:
        batch_op.alter_column(
            "task_id", existing_type=sa.INTEGER(), nullable=True
        )
        batch_op.alter_column(
            "workflow_id", existing_type=sa.INTEGER(), nullable=True
        )

    # ### end Alembic commands ###
