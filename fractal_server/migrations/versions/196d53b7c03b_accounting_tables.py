"""accounting tables

Revision ID: 196d53b7c03b
Revises: 1eac13a26c83
Create Date: 2025-02-10 10:41:25.230784

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "196d53b7c03b"
down_revision = "1eac13a26c83"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "accounting",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("num_tasks", sa.Integer(), nullable=False),
        sa.Column("num_new_images", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user_oauth.id"],
            name=op.f("fk_accounting_user_id_user_oauth"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_accounting")),
    )
    op.create_table(
        "accountingslurm",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "slurm_job_ids", postgresql.ARRAY(sa.Integer()), nullable=True
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user_oauth.id"],
            name=op.f("fk_accountingslurm_user_id_user_oauth"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_accountingslurm")),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("accountingslurm")
    op.drop_table("accounting")
    # ### end Alembic commands ###
