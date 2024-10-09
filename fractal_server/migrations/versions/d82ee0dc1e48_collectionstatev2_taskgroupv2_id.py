"""CollectionStateV2.taskgroupv2_id

Revision ID: d82ee0dc1e48
Revises: 742b74e1cc6e
Create Date: 2024-10-09 14:13:59.288582

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "d82ee0dc1e48"
down_revision = "742b74e1cc6e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("collectionstatev2", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("taskgroupv2_id", sa.Integer(), nullable=True)
        )
        batch_op.create_foreign_key(
            batch_op.f("fk_collectionstatev2_taskgroupv2_id_taskgroupv2"),
            "taskgroupv2",
            ["taskgroupv2_id"],
            ["id"],
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("collectionstatev2", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("fk_collectionstatev2_taskgroupv2_id_taskgroupv2"),
            type_="foreignkey",
        )
        batch_op.drop_column("taskgroupv2_id")

    # ### end Alembic commands ###