"""JSON to JSONB

Revision ID: b3ffb095f973
Revises: b1e7f7a1ff71
Create Date: 2025-06-19 10:12:06.699107

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "b3ffb095f973"
down_revision = "b1e7f7a1ff71"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("datasetv2", schema=None) as batch_op:
        batch_op.alter_column(
            "history",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )
        batch_op.alter_column(
            "images",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    with op.batch_alter_table("jobv2", schema=None) as batch_op:
        batch_op.alter_column(
            "dataset_dump",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "workflow_dump",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "project_dump",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "attribute_filters",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'{}'::json"),
        )
        batch_op.alter_column(
            "type_filters",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'{}'::json"),
        )

    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            "pinned_package_versions",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=True,
            existing_server_default=sa.text("'{}'::json"),
        )

    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "input_types",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "output_types",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "tags",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    with op.batch_alter_table("user_settings", schema=None) as batch_op:
        batch_op.alter_column(
            "slurm_accounts",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    with op.batch_alter_table("usergroup", schema=None) as batch_op:
        batch_op.alter_column(
            "viewer_paths",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    with op.batch_alter_table("workflowtaskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "args_parallel",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "args_non_parallel",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "type_filters",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            type_=postgresql.JSONB(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'{}'::json"),
        )

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("workflowtaskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "type_filters",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'{}'::json"),
        )
        batch_op.alter_column(
            "args_non_parallel",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "args_parallel",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=True,
        )

    with op.batch_alter_table("usergroup", schema=None) as batch_op:
        batch_op.alter_column(
            "viewer_paths",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    with op.batch_alter_table("user_settings", schema=None) as batch_op:
        batch_op.alter_column(
            "slurm_accounts",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.alter_column(
            "tags",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )
        batch_op.alter_column(
            "output_types",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "input_types",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=True,
        )

    with op.batch_alter_table("taskgroupv2", schema=None) as batch_op:
        batch_op.alter_column(
            "pinned_package_versions",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=True,
            existing_server_default=sa.text("'{}'::json"),
        )

    with op.batch_alter_table("jobv2", schema=None) as batch_op:
        batch_op.alter_column(
            "type_filters",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'{}'::json"),
        )
        batch_op.alter_column(
            "attribute_filters",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'{}'::json"),
        )
        batch_op.alter_column(
            "project_dump",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "workflow_dump",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
        )
        batch_op.alter_column(
            "dataset_dump",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
        )

    with op.batch_alter_table("datasetv2", schema=None) as batch_op:
        batch_op.alter_column(
            "images",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )
        batch_op.alter_column(
            "history",
            existing_type=postgresql.JSONB(astext_type=sa.Text()),
            type_=postgresql.JSON(astext_type=sa.Text()),
            existing_nullable=False,
            existing_server_default=sa.text("'[]'::json"),
        )

    # ### end Alembic commands ###
