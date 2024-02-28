"""v2

Revision ID: 19fe314f8fd2
Revises: 9fd26a2b0de4
Create Date: 2024-02-27 16:45:17.810981

"""
import sqlalchemy as sa
import sqlmodel
from alembic import op


# revision identifiers, used by Alembic.
revision = "19fe314f8fd2"
down_revision = "9fd26a2b0de4"
branch_labels = None
depends_on = None


def upgrade() -> None:

    op.create_table(
        "taskv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("is_parallel", sa.Boolean(), nullable=False),
        sa.Column(
            "command", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column(
            "source", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.Column("owner", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column(
            "version", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column("args_schema", sa.JSON(), nullable=True),
        sa.Column(
            "args_schema_version",
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=True,
        ),
        sa.Column(
            "docs_info", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "docs_link", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source"),
    )
    op.create_table(
        "datasetv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.Column("history", sa.JSON(), server_default="[]", nullable=False),
        sa.Column("read_only", sa.Boolean(), nullable=False),
        sa.Column(
            "timestamp_created", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column("images", sa.JSON(), server_default="[]", nullable=False),
        sa.Column("filters", sa.JSON(), server_default="{}", nullable=False),
        sa.Column("buffer", sa.JSON(), nullable=True),
        sa.Column("parallelization_list", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["project.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "workflowv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column(
            "timestamp_created", sa.DateTime(timezone=True), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["project.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "jobv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_id", sa.Integer(), nullable=True),
        sa.Column("workflow_id", sa.Integer(), nullable=True),
        sa.Column("dataset_id", sa.Integer(), nullable=True),
        sa.Column(
            "user_email", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column(
            "slurm_account", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column("dataset_dump", sa.JSON(), nullable=False),
        sa.Column("workflow_dump", sa.JSON(), nullable=False),
        sa.Column("project_dump", sa.JSON(), nullable=False),
        sa.Column(
            "worker_init", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "working_dir", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "working_dir_user",
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=True,
        ),
        sa.Column("first_task_index", sa.Integer(), nullable=False),
        sa.Column("last_task_index", sa.Integer(), nullable=False),
        sa.Column(
            "start_timestamp", sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column("end_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column("log", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["datasetv2.id"],
        ),
        sa.ForeignKeyConstraint(
            ["project_id"],
            ["project.id"],
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflowv2.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "workflowtaskv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("workflow_id", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.Integer(), nullable=False),
        sa.Column("order", sa.Integer(), nullable=True),
        sa.Column("meta", sa.JSON(), nullable=True),
        sa.Column("args", sa.JSON(), nullable=True),
        sa.Column("filters", sa.JSON(), server_default="{}", nullable=False),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["taskv2.id"],
        ),
        sa.ForeignKeyConstraint(
            ["workflow_id"],
            ["workflowv2.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "version", sqlmodel.sql.sqltypes.AutoString(), nullable=False
            )
        )
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.alter_column("version", server_default=None)


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("project", schema=None) as batch_op:
        batch_op.drop_column("version")

    op.drop_table("workflowtaskv2")
    op.drop_table("jobv2")
    op.drop_table("workflowv2")
    op.drop_table("datasetv2")
    op.drop_table("taskv2")
    # ### end Alembic commands ###
