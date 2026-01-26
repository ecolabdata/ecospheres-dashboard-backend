"""Add Stats.segment column

Revision ID: e80f9232f15c
Revises: 90f327cf1916
Create Date: 2026-01-26 15:41:08.997903

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e80f9232f15c"
down_revision: Union[str, None] = "90f327cf1916"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("stats", sa.Column("segment", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("stats", "segment")
