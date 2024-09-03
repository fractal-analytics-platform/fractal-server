"""Remove WorkflowTaskV2 legacy attributes

Revision ID: 40b3479ba47f
Revises: 5bf02391cfef
Create Date: 2024-09-03 10:11:27.477326

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "40b3479ba47f"
down_revision = "5bf02391cfef"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "fk_workflowtaskv2_task_legacy_id_task",
        "workflowtaskv2",
        type_="foreignkey",
    )
    op.drop_column("workflowtaskv2", "is_legacy_task")
    op.drop_column("workflowtaskv2", "task_legacy_id")
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "workflowtaskv2",
        sa.Column("task_legacy_id", sa.INTEGER(), nullable=True),
    )
    op.add_column(
        "workflowtaskv2",
        sa.Column("is_legacy_task", sa.BOOLEAN(), nullable=False),
    )
    op.create_foreign_key(
        "fk_workflowtaskv2_task_legacy_id_task",
        "workflowtaskv2",
        "task",
        ["task_legacy_id"],
        ["id"],
    )
    # ### end Alembic commands ###
