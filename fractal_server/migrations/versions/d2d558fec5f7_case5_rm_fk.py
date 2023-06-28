"""case5_rm_fk

Revision ID: d2d558fec5f7
Revises: 8372224120ba
Create Date: 2023-06-28 10:18:41.077730

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "d2d558fec5f7"
down_revision = "8372224120ba"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("resource", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_resource_dataset_id_dataset", type_="foreignkey"
        )
        batch_op.drop_column("dataset_id")

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("resource", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("dataset_id", sa.INTEGER(), nullable=False)
        )
        batch_op.create_foreign_key(
            "fk_resource_dataset_id_dataset", "dataset", ["dataset_id"], ["id"]
        )

    # ### end Alembic commands ###
