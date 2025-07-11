"""task groups

Revision ID: 034a469ec2eb
Revises: da2cb2ac4255
Create Date: 2024-10-10 16:14:13.976231

"""
from datetime import datetime
from datetime import timezone

import sqlalchemy as sa
import sqlmodel
from alembic import op


# revision identifiers, used by Alembic.
revision = "034a469ec2eb"
down_revision = "da2cb2ac4255"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "taskgroupv2",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("user_group_id", sa.Integer(), nullable=True),
        sa.Column(
            "origin", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column(
            "pkg_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False
        ),
        sa.Column(
            "version", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "python_version", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column("path", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column(
            "venv_path", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "wheel_path", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "pip_extras", sqlmodel.sql.sqltypes.AutoString(), nullable=True
        ),
        sa.Column(
            "pinned_package_versions",
            sa.JSON(),
            server_default="{}",
            nullable=True,
        ),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "timestamp_created", sa.DateTime(timezone=True), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["user_group_id"],
            ["usergroup.id"],
            name=op.f("fk_taskgroupv2_user_group_id_usergroup"),
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["user_oauth.id"],
            name=op.f("fk_taskgroupv2_user_id_user_oauth"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_taskgroupv2")),
    )
    with op.batch_alter_table("collectionstatev2", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("taskgroupv2_id", sa.Integer(), nullable=True)
        )
        batch_op.create_foreign_key(
            batch_op.f("fk_collectionstatev2_taskgroupv2_id_taskgroupv2"),
            "taskgroupv2",
            ["taskgroupv2_id"],
            ["id"],
        )

    with op.batch_alter_table("linkusergroup", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "timestamp_created",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=str(datetime(2000, 1, 1, tzinfo=timezone.utc)),
            )
        )

    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("taskgroupv2_id", sa.Integer(), nullable=True)
        )
        batch_op.add_column(
            sa.Column(
                "category", sqlmodel.sql.sqltypes.AutoString(), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "modality", sqlmodel.sql.sqltypes.AutoString(), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column(
                "authors", sqlmodel.sql.sqltypes.AutoString(), nullable=True
            )
        )
        batch_op.add_column(
            sa.Column("tags", sa.JSON(), server_default="[]", nullable=False)
        )
        batch_op.alter_column(
            "source", existing_type=sa.VARCHAR(), nullable=True
        )

    try:
        with op.batch_alter_table("taskv2", schema=None) as batch_op:
            batch_op.drop_constraint("uq_taskv2_source", type_="unique")
    except BaseException as e:
        if op.get_bind().dialect.name != "sqlite":
            raise e
        import sqlite3
        import logging

        logger = logging.getLogger("alembic.runtime.migration")
        logger.warning(
            f"Using sqlite, with {sqlite3.version=} and "
            f"{sqlite3.sqlite_version=}"
        )

        logger.warning(
            "Could not drop 'uq_taskv2_source' constraint; this is expected "
            "when the database was created before the naming convention "
            "was added."
        )
        logger.warning(
            "As a workaround, we recreate the constraint before dropping it."
        )
        with op.batch_alter_table("taskv2", schema=None) as batch_op:
            batch_op.create_unique_constraint("uq_taskv2_source", ["source"])
            batch_op.drop_constraint("uq_taskv2_source", type_="unique")

    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.create_foreign_key(
            batch_op.f("fk_taskv2_taskgroupv2_id_taskgroupv2"),
            "taskgroupv2",
            ["taskgroupv2_id"],
            ["id"],
        )


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("taskv2", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("fk_taskv2_taskgroupv2_id_taskgroupv2"),
            type_="foreignkey",
        )
        batch_op.create_unique_constraint("uq_taskv2_source", ["source"])
        batch_op.alter_column(
            "source", existing_type=sa.VARCHAR(), nullable=False
        )
        batch_op.drop_column("tags")
        batch_op.drop_column("authors")
        batch_op.drop_column("modality")
        batch_op.drop_column("category")
        batch_op.drop_column("taskgroupv2_id")

    with op.batch_alter_table("linkusergroup", schema=None) as batch_op:
        batch_op.drop_column("timestamp_created")

    with op.batch_alter_table("collectionstatev2", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("fk_collectionstatev2_taskgroupv2_id_taskgroupv2"),
            type_="foreignkey",
        )
        batch_op.drop_column("taskgroupv2_id")

    op.drop_table("taskgroupv2")
    # ### end Alembic commands ###
