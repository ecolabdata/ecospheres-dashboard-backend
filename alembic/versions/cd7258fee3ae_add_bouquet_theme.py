"""Add Bouquet.theme

Revision ID: cd7258fee3ae
Revises: eb04e2618b0f
Create Date: 2025-06-30 12:06:33.864018

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "cd7258fee3ae"
down_revision: Union[str, None] = "eb04e2618b0f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("bouquets", sa.Column("theme", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("bouquets", "theme")
