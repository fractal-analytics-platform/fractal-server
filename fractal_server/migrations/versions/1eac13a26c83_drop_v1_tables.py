"""Drop V1 tables

Revision ID: 1eac13a26c83
Revises: af8673379a5c
Create Date: 2025-01-10 13:17:47.838607

"""
import logging

from alembic import op
from sqlmodel import SQLModel

from fractal_server.migrations.naming_convention import NAMING_CONVENTION

# revision identifiers, used by Alembic.
revision = "1eac13a26c83"
down_revision = "af8673379a5c"
branch_labels = None
depends_on = None


TABLES_V1 = [
    "resource",
    "applyworkflow",
    "task",
    "workflow",
    "workflowtask",
    "linkuserproject",
    "dataset",
    "project",
    "state",
]


def upgrade() -> None:
    logger = logging.getLogger("alembic.runtime.migration")

    target_metadata = SQLModel.metadata
    target_metadata.naming_convention = NAMING_CONVENTION

    connection = op.get_bind()
    target_metadata.reflect(
        bind=connection,
        extend_existing=True,
        only=TABLES_V1,
    )

    logger.info("Starting non-reversible upgrade")
    logger.info("Dropping all V1 ForeignKey constraints")
    fk_names = []
    for table_name in TABLES_V1:
        table = target_metadata.tables[table_name]
        for fk in table.foreign_keys:
            op.drop_constraint(fk.name, table_name, type_="foreignkey")
            fk_names.append(fk.name)
    logger.info(f"Dropped all V1 ForeignKey constraints: {fk_names}")
    logger.info(f"Dropping all V1 tables: {TABLES_V1}")
    for table_name in TABLES_V1:
        op.drop_table(table_name)


def downgrade() -> None:
    raise RuntimeError(
        "Cannot downgrade from 1eac13a26c83 to db09233ad13a, "
        "because it's fully breaking."
    )
