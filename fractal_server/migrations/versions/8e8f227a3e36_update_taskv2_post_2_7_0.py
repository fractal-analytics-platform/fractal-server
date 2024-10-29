"""Update TaskV2 post 2.7.0

Revision ID: 8e8f227a3e36
Revises: 034a469ec2eb
Create Date: 2024-10-29 09:01:33.075251

"""
import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "8e8f227a3e36"
down_revision = "034a469ec2eb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "taskgroupv2_id", existing_type=sa.INTEGER(), nullable=False
        )
        batch_op.drop_column("owner")

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "owner", sa.VARCHAR(), autoincrement=False, nullable=True
            )
        )
        batch_op.alter_column(
            "taskgroupv2_id", existing_type=sa.INTEGER(), nullable=True
        )

    # ### end Alembic commands ###
