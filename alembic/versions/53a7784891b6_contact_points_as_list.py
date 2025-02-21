"""contact_points as list

Revision ID: 53a7784891b6
Revises: af780a5cffbe
Create Date: 2025-02-21 12:55:31.873698

Changes:
- Migrate dataset.contact_point to dataset.contact_points[]
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "53a7784891b6"
down_revision: Union[str, None] = "af780a5cffbe"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()

    op.add_column("catalog", sa.Column("contact_points", JSONB))
    conn.execute(
        sa.text("""
            UPDATE catalog
            SET contact_points
                CASE
                    WHEN contact_point IS NULL THEN NULL
                    ELSE jsonb_build_array(contact_point)
                END
        """)
    )
    op.drop_column("catalog", "contact_point")

    op.alter_column("catalog", "has_contact_point", new_column_name="has_contact_points")


def downgrade() -> None:
    conn = op.get_bind()

    op.add_column("catalog", sa.Column("contact_points", JSONB))
    # WARNING: lossy
    conn.execute(
        sa.text("""
            UPDATE catalog
            SET contact_point =
                CASE
                    WHEN contact_points IS NULL THEN NULL
                    WHEN jsonb_array_length(contact_points) > 0 THEN contact_points->0
                    ELSE NULL
                END
        """)
    )
    op.drop_column("catalog", "contact_points")

    op.alter_column("catalog", "has_contact_points", new_column_name="has_contact_point")
