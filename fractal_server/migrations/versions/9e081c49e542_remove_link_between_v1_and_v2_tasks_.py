"""Remove link between v1 and v2 tasks/workflowtasks tables

Revision ID: 9e081c49e542
Revises: 5bf02391cfef
Create Date: 2024-09-04 08:44:12.471028

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "9e081c49e542"
down_revision = "5bf02391cfef"
branch_labels = None
depends_on = None


def upgrade() -> None:

    # We modified the auto generated commands because sqlite does not support
    # "alter table". They would have worked fine with postgresql.

    # ### commands auto generated by Alembic - please adjust! ###
    # op.drop_column("task", "is_v2_compatible")
    # op.alter_column(
    #     "workflowtaskv2",
    #     "task_id",
    #     existing_type=sa.INTEGER(),
    #     nullable=False,
    # )
    # op.drop_constraint(
    #     "fk_workflowtaskv2_task_legacy_id_task",
    #     "workflowtaskv2",
    #     type_="foreignkey",
    # )
    # op.drop_column("workflowtaskv2", "task_legacy_id")
    # op.drop_column("workflowtaskv2", "is_legacy_task")
    # ### end Alembic commands ###

    with op.batch_alter_table("task", schema=None) as batch_op:
        batch_op.drop_column("is_v2_compatible")

    with op.batch_alter_table("workflowtaskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "task_id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.drop_column("task_legacy_id")
        batch_op.drop_column("is_legacy_task")


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "workflowtaskv2",
        sa.Column("is_legacy_task", sa.BOOLEAN(), nullable=False),
    )
    op.add_column(
        "workflowtaskv2",
        sa.Column("task_legacy_id", sa.INTEGER(), nullable=True),
    )
    op.create_foreign_key(
        "fk_workflowtaskv2_task_legacy_id_task",
        "workflowtaskv2",
        "task",
        ["task_legacy_id"],
        ["id"],
    )
    op.alter_column(
        "workflowtaskv2", "task_id", existing_type=sa.INTEGER(), nullable=True
    )
    op.add_column(
        "task",
        sa.Column(
            "is_v2_compatible",
            sa.BOOLEAN(),
            server_default=sa.text("(false)"),
            nullable=False,
        ),
    )
    # ### end Alembic commands ###
