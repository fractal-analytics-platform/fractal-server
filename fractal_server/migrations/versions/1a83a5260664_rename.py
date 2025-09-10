"""Rename `pinned_package_versions`

Revision ID: 1a83a5260664
Revises: b3ffb095f973
Create Date: 2025-09-10 14:16:51.202765

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "1a83a5260664"
down_revision = "b3ffb095f973"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            column_name="pinned_package_versions",
            new_column_name="pinned_package_versions_post",
        )


def downgrade() -> None:
    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            column_name="pinned_package_versions_post",
            new_column_name="pinned_package_versions",
        )
