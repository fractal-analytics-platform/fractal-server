"""add slurm_accounts

Revision ID: 71eefd1dd202
Revises: d4fe3708d309
Create Date: 2023-12-05 12:36:44.100065

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op


# revision identifiers, used by Alembic.
revision = "71eefd1dd202"
down_revision = "d4fe3708d309"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "slurm_account",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=True,
            )
        )

    with op.batch_alter_table("user_oauth", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "slurm_accounts",
                sa.JSON(),
                server_default="[]",
                nullable=False,
            )
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("user_oauth", schema=None) as batch_op:
        batch_op.drop_column("slurm_accounts")

    with op.batch_alter_table("applyworkflow", schema=None) as batch_op:
        batch_op.drop_column("slurm_account")

    # ### end Alembic commands ###