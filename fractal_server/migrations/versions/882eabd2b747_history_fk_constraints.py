"""history fk constraints

Revision ID: 882eabd2b747
Revises: c743c9f9205f
Create Date: 2025-03-14 15:21:06.813903

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "882eabd2b747"
down_revision = "c743c9f9205f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("historyimagecache", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_historyimagecache_workflowtask_id_workflowtaskv2",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            batch_op.f("fk_historyimagecache_workflowtask_id_workflowtaskv2"),
            "workflowtaskv2",
            ["workflowtask_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("historyrun", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_historyrun_workflowtask_id_workflowtaskv2", type_="foreignkey"
        )
        batch_op.create_foreign_key(
            batch_op.f("fk_historyrun_workflowtask_id_workflowtaskv2"),
            "workflowtaskv2",
            ["workflowtask_id"],
            ["id"],
            ondelete="SET NULL",
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("historyrun", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("fk_historyrun_workflowtask_id_workflowtaskv2"),
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_historyrun_workflowtask_id_workflowtaskv2",
            "workflowtaskv2",
            ["workflowtask_id"],
            ["id"],
        )

    with op.batch_alter_table("historyimagecache", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("fk_historyimagecache_workflowtask_id_workflowtaskv2"),
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_historyimagecache_workflowtask_id_workflowtaskv2",
            "workflowtaskv2",
            ["workflowtask_id"],
            ["id"],
        )

    # ### end Alembic commands ###
