"""Add HistoryItem table

Revision ID: 87cd72a537a2
Revises: af1ef1c83c9b
Create Date: 2025-02-18 10:48:16.401995

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "87cd72a537a2"
down_revision = "af1ef1c83c9b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "historyitemv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("workflowtask_id", sa.Integer(), nullable=True),
        sa.Column(
            "timestamp_started", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            "workflowtask_dump",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "task_group_dump",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "parameters_hash",
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=False,
        ),
        sa.Column("num_available_images", sa.Integer(), nullable=False),
        sa.Column("num_current_images", sa.Integer(), nullable=False),
        sa.Column(
            "images", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["datasetv2.id"],
            name=op.f("fk_historyitemv2_dataset_id_datasetv2"),
        ),
        sa.ForeignKeyConstraint(
            ["workflowtask_id"],
            ["workflowtaskv2.id"],
            name=op.f("fk_historyitemv2_workflowtask_id_workflowtaskv2"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_historyitemv2")),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("historyitemv2")
    # ### end Alembic commands ###
