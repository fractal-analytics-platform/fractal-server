"""Task group for pixi

Revision ID: b1e7f7a1ff71
Revises: 791ce783d3d8
Create Date: 2025-05-29 16:31:17.565973

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op


# revision identifiers, used by Alembic.
revision = "b1e7f7a1ff71"
down_revision = "791ce783d3d8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "pixi_version",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=True,
            )
        )
        batch_op.alter_column(
            "wheel_path",
            nullable=True,
            new_column_name="archive_path",
        )
        batch_op.alter_column(
            "pip_freeze",
            nullable=True,
            new_column_name="env_info",
        )


def downgrade() -> None:
    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            "archive_path",
            nullable=True,
            new_column_name="wheel_path",
        )
        batch_op.alter_column(
            "env_info",
            nullable=True,
            new_column_name="pip_freeze",
        )
        batch_op.drop_column("pixi_version")
