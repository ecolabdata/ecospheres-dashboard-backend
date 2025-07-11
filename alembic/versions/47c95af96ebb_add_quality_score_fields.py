"""Add quality score fields

Revision ID: 47c95af96ebb
Revises: 3dd5e076e921
Create Date: 2025-07-11 16:33:36.161546

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "47c95af96ebb"
down_revision: Union[str, None] = "3dd5e076e921"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("catalog", sa.Column("quality__score__bin", sa.Integer(), nullable=False))
    op.add_column("catalog", sa.Column("quality__score__bin_label", sa.String(), nullable=False))


def downgrade() -> None:
    op.drop_column("catalog", "quality__score__bin")
    op.drop_column("catalog", "quality__score__bin_label")
