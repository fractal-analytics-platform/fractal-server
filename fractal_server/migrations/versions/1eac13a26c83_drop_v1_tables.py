"""Drop V1 tables

Revision ID: 1eac13a26c83
Revises: db09233ad13a
Create Date: 2025-01-10 13:17:47.838607

"""
import logging

from alembic import op
from sqlmodel import SQLModel

from fractal_server.migrations.naming_convention import NAMING_CONVENTION

# revision identifiers, used by Alembic.
revision = "1eac13a26c83"
down_revision = "db09233ad13a"
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
    target_metadata = SQLModel.metadata
    target_metadata.naming_convention = NAMING_CONVENTION

    connection = op.get_bind()
    target_metadata.reflect(bind=connection, extend_existing=True)

    for table_name in TABLES_V1:
        table = target_metadata.tables[table_name]
        for fk in table.foreign_keys:
            logging.warning(f"Dropping FK constraint {fk.name}")
            op.drop_constraint(fk.name, table_name, type_="foreignkey")

    for table_name in TABLES_V1:
        logging.warning(f"Dropping table {table_name}")
        op.drop_table(table_name)


def downgrade() -> None:
    pass
