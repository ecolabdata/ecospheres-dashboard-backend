"""contact_points as list

Revision ID: 84c4d5a939e5
Revises: 4eb628a9e208
Create Date: 2025-02-27 01:02:56.425913

Changes:
- Migrate dataset.contact_point to dataset.contact_points[]
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "84c4d5a939e5"
down_revision: Union[str, None] = "4eb628a9e208"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    # `nullable=False` => needs `server_default` to handle existing data
    # https://stackoverflow.com/a/33705698
    op.add_column(
        "catalog",
        sa.Column("contact_points", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
    )
    conn.execute(
        sa.text("""
            UPDATE catalog
            SET contact_points =
                CASE
                    WHEN contact_point IS NULL THEN jsonb_build_array()
                    ELSE jsonb_build_array(contact_point)
                END
        """)
    )
    op.alter_column("catalog", "contact_points", server_default=None)
    op.drop_column("catalog", "contact_point")

    op.alter_column("catalog", "has_contact_point", new_column_name="has_contact_points")


def downgrade() -> None:
    conn = op.get_bind()

    op.add_column("catalog", sa.Column("contact_points", JSONB))
    # WARNING: lossy, and expecting nullable=False
    conn.execute(
        sa.text("""
            UPDATE catalog
            SET contact_point =
                CASE
                    WHEN jsonb_array_length(contact_points) > 0 THEN contact_points->0
                    ELSE NULL
                END
        """)
    )
    op.drop_column("catalog", "contact_points")

    op.alter_column("catalog", "has_contact_points", new_column_name="has_contact_point")
