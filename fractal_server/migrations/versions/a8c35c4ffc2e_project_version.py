"""project version

Revision ID: a8c35c4ffc2e
Revises: 9fd26a2b0de4
Create Date: 2024-02-26 11:08:43.291289

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op


# revision identifiers, used by Alembic.
revision = "a8c35c4ffc2e"
down_revision = "9fd26a2b0de4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "version",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=False,
                server_default="v1",
            )
        )

    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.alter_column("version", server_default=None)

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.drop_column("version")

    # ### end Alembic commands ###
